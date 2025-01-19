import asyncio
import logging
from datetime import datetime, timezone
import os
from logging.handlers import RotatingFileHandler
from automation_check import main

# 로그 디렉토리 생성
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 로그 설정
def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 파일 핸들러 (로그 파일)
    log_file = os.path.join(log_dir, f'{name}.log')
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,
        backupCount=5
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger('market_automation_check')

async def run_with_retry(max_retries=3):
    for attempt in range(max_retries):
        try:
            return await main()
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            logger.exception("상세 에러:")
            if attempt < max_retries - 1:
                await asyncio.sleep(60)
            else:
                raise

async def scheduler():
    while True:
        try:
            start_time = datetime.now(timezone.utc)
            logger.info(f"Starting execution at {start_time}")
            
            orders = await run_with_retry()
            logger.info(f"Processed orders: {orders}")
            
            logger.info(f"Completed execution at {datetime.now(timezone.utc)}")
            await asyncio.sleep(1800)
            
        except Exception as e:
            logger.error(f"Critical error occurred: {e}")
            logger.exception("상세 에러:")
            await asyncio.sleep(60)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        logger.info("서비스 시작")
        loop.run_until_complete(scheduler())
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler stopped due to error: {e}")
        logger.exception("상세 에러:")
    finally:
        logger.info("서비스 종료")
        loop.close()