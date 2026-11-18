import os,json,logging,socket,threading,time
from datetime import datetime,timezone
from pathlib import Path
import redis
from sqlalchemy import select,update
from sqlalchemy.exc import OperationalError,InterfaceError
from .db import Session,ensure_columns
from .models import Layout
from .selection import calculate
from .services import selected,source_checksum
QUEUE='tunnel-selection'
LEASE_TTL=30    # 执行者租约秒数，计算期间心跳续期
GRACE_FILE=3    # 结果已入库、文件未确认时的接管宽限秒数
GRACE_QUEUE=12  # 等待/计算中任务的接管宽限秒数
EXECUTOR=f'{socket.gethostname()}:{os.getpid()}'
r=redis.Redis.from_url(os.environ['REDIS_URL'],decode_responses=True)
now=lambda:datetime.now(timezone.utc)
iso=lambda:now().isoformat()
def push(lid,attempt):r.rpush(QUEUE,json.dumps({'id':lid,'attempt':attempt}))
# 栅栏发布：仅当前attempt且仍在计算中的行可被改写，接管后旧执行者的迟回结果全部落空
def fenced(s,lid,attempt,**values):
    return s.execute(update(Layout).where(Layout.id==lid,Layout.compute_attempt==attempt,Layout.status=='calculating').values(**values)).rowcount
def write_file(lid,evidence):
    p=Path(f'/artifacts/layout-{lid}.json');t=p.with_name(p.name+'.tmp')
    t.write_text(json.dumps(evidence,ensure_ascii=False));os.replace(t,p)
# 可审签门槛：数据库证据、实际选点、计算文件三者一致
def consistent(s,l):
    ev=l.evidence or {}
    if not ev.get('selected_probe_ids'):return False,'计算证据不完整'
    if ev['selected_probe_ids']!=selected(s,l.id):return False,'选点与计算证据不一致'
    if l.source_checksum!=source_checksum(s):return False,'来源校验已变化'
    try:disk=json.loads(Path(f'/artifacts/layout-{l.id}.json').read_text())
    except Exception:return False,'计算文件缺失或不可读'
    if disk!=ev:return False,'计算文件与数据库证据不一致'
    return True,''
def fail_fenced(lid,attempt,error):
    with Session() as s:
        if fenced(s,lid,attempt,status='failed',compute_phase=None,compute_owner=None,compute_note=('计算失败：'+error)[:190],evidence={'error':error})==1:s.commit()
        else:s.rollback();logging.warning('layout %s attempt %s 已被接管，迟回的失败不再改写',lid,attempt)
# 结果入库后统一收尾：补写/核对计算文件，一致才进入待审签；不写库外新计算、不改选点
def finalize(lid,attempt,repaired=False):
    with Session() as s:
        l=s.get(Layout,lid)
        if not l or l.status!='calculating' or l.compute_attempt!=attempt or not l.evidence:return
        try:write_file(lid,l.evidence)
        except Exception:logging.exception('layout %s 计算文件写入失败，等待接管补写',lid);return
        ok,reason=consistent(s,l)
        if not ok:
            if '文件' in reason:return  # 文件仍不可读，留待巡检按同一证据补写
            if fenced(s,lid,attempt,status='failed',compute_phase=None,compute_owner=None,compute_note=reason,evidence={'error':reason})==1:s.commit()
            else:s.rollback()
            return
        note=('补写计算文件后恢复，' if repaired else '')+f'第{attempt}次执行完成，数据库、选点与计算文件一致'
        if fenced(s,lid,attempt,status='ready',compute_phase=None,compute_owner=None,compute_note=note,compute_stamp=iso())==1:s.commit()
        else:s.rollback();logging.warning('layout %s attempt %s 发布前已被接管',lid,attempt)
def run_compute(lid,attempt):
    with Session() as s:
        l=s.get(Layout,lid)
        try:calculate(s,l)
        except (OperationalError,InterfaceError):s.rollback();raise  # 瞬时库故障留给巡检接管重算
        except Exception as ex:
            s.rollback();fail_fenced(lid,attempt,str(ex));return
        if fenced(s,lid,attempt,compute_phase='finalizing',compute_stamp=iso())!=1:
            s.rollback();logging.warning('layout %s attempt %s 已被接管，迟回的结果不再发布',lid,attempt);return
        s.commit()
    finalize(lid,attempt)
def beat(stop,key,token):
    while not stop.wait(5):
        try:r.set(key,token,ex=LEASE_TTL)
        except Exception:return
def handle(msg):
    lid,attempt=msg.get('id'),msg.get('attempt')
    with Session() as s:
        l=s.get(Layout,lid)
        if not l or l.status!='calculating':return  # 重复或已完结的消息直接丢弃
        if attempt is None:attempt=l.compute_attempt
        if l.compute_attempt!=attempt:return        # 过期attempt的重复消息
        repair=l.compute_phase=='repairing' or bool(l.evidence and l.source_checksum)
        n=s.execute(update(Layout).where(Layout.id==lid,Layout.compute_attempt==attempt,Layout.status=='calculating',Layout.compute_phase.in_(['queued','taken_over','repairing'])).values(compute_phase='repairing' if repair else 'running',compute_owner=EXECUTOR,compute_stamp=iso())).rowcount
        if n!=1:s.rollback();return                 # 已有执行者认领
        s.commit()
    key=f'tunnel-lease:{lid}:{attempt}'    # 租约按attempt隔离，旧执行者心跳不会干扰新执行者
    try:r.set(key,EXECUTOR,ex=LEASE_TTL)
    except Exception:pass
    stop=threading.Event();threading.Thread(target=beat,args=(stop,key,EXECUTOR),daemon=True).start()
    try:
        if repair:finalize(lid,attempt,repaired=True)   # 同一次计算只补文件，不重算不改点
        else:run_compute(lid,attempt)
    finally:
        stop.set()
        try:r.delete(key)
        except Exception:pass
# 巡检：计算中但执行者租约失效的任务自动接管重投；结果已入库的只补文件。消息丢失不漏算，重复不多发
def janitor():
    try:
        with Session() as s:
            for l in s.scalars(select(Layout).where(Layout.status=='calculating')).all():
                try:token=r.get(f'tunnel-lease:{l.id}:{l.compute_attempt}')
                except Exception:return
                if token:continue
                try:age=(now()-datetime.fromisoformat(l.compute_stamp)).total_seconds() if l.compute_stamp else 9e9
                except Exception:age=9e9
                if age<(GRACE_FILE if l.compute_phase in ('finalizing','repairing') else GRACE_QUEUE):continue
                attempt=(l.compute_attempt or 0)+1
                repair=bool(l.evidence and l.source_checksum)
                reason='结果已入库但计算文件未确认，接管补写' if repair else '原执行者中断或队列消息丢失，接管重新计算'
                n=fenced(s,l.id,l.compute_attempt,compute_attempt=attempt,compute_phase='repairing' if repair else 'taken_over',compute_note=f'{reason}（第{attempt}次执行）',compute_owner=None,compute_stamp=iso())
                if n!=1:s.rollback();continue
                s.commit()
                try:push(l.id,attempt)
                except Exception:logging.exception('layout %s 重新入队失败，下次巡检重试',l.id)
    except Exception:logging.exception('janitor pass failed')
def main():
    ensure_columns();Path('/artifacts').mkdir(exist_ok=True);last=0.0
    while True:
        Path('/tmp/worker-heartbeat').touch()
        try:entry=r.blpop(QUEUE,timeout=2)
        except Exception:logging.exception('queue read failed');time.sleep(2);continue
        if entry:
            try:msg=json.loads(entry[1])
            except Exception:
                try:msg={'id':int(entry[1]),'attempt':None}
                except Exception:continue
            if not isinstance(msg,dict):msg={'id':msg,'attempt':None}
            try:handle(msg)
            except Exception:logging.exception('handle failed')
        if time.monotonic()-last>4:last=time.monotonic();janitor()
if __name__=='__main__':main()
