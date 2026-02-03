# -*- coding: utf-8 -*-

from typing import Optional
from multiprocessing import Pool
import atexit
from executor.exec_script import exec_script


_worker_pool: Optional[Pool] = None


def init_worker_pool(worker_num: int):
    """
    进程启动时调用一次
    """
    global _worker_pool
    if _worker_pool is None:
        _worker_pool = Pool(worker_num)
        atexit.register(close_worker_pool)


def close_worker_pool():
    global _worker_pool
    if _worker_pool:
        _worker_pool.close()
        _worker_pool.join()
        _worker_pool = None


def run_worker(task: dict) -> dict:
    """
    同步执行单个任务
    """
    if _worker_pool is None:
        raise RuntimeError("worker pool not initialized")

    async_result = _worker_pool.apply_async(_worker, (task,))
    return async_result.get()   # 阻塞直到完成


def _worker(task: dict) -> dict:
    try:
        result = exec_script(
            task["script"],
            task["data"],
            task["context"]
        )
        return {"status": "SUCCESS", "result": result}
    except Exception as e:
        return {"status": "FAIL", "error": str(e)}


