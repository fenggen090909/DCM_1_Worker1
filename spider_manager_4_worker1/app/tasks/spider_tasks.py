from app import celery
import subprocess
import os
import logging
import sys
# from app.utils.logging import get_logging
from app.config import Config
import socket
from scrapy.crawler import CrawlerProcess
import socket
# from scrapy.utils.project import get_project_settings
# import app.celeryconfig 

# 初始化日志系统 - 关键步骤
# logging = get_logging(__name__)

# @celery.task(bind=True)
# def get_worker_ip(self):
#     """获取worker的ip地址"""       
#     hostname = socket.gethostname()
#     ip_address = socket.gethostbyname(hostname)
#     logging.info(f"worker hostname: {hostname}")
#     logging.info(f"worker ip address: {ip_address}")
#     return ip_address, hostname


@celery.task(bind=True, queue='1_queue')
def run_crawler_task(self, spider_name, start_page, end_page):
    """运行Scrapy爬虫的Celery任务"""
    
    # 关键步骤2: 确保 logging 设置正确的级别和处理器
    # logging.setLevel(logging.INFO)

    # ip, hostname = get_worker_ip()
 
    hostname = socket.gethostname()

    logging.info(f"fg **** taskid={self.request.id}, args={self.request.args} kwargs={self.request.kwargs}")
    logging.info(f"fg **** workerid={self.request.hostname} hostname={hostname}")
    # logging.info(f"fg **** ip={ip} hostname={hostname}")              
   
    
    logging.info(f"======= start - 爬虫: {spider_name} =======")
    
    try:
        # 记录环境信息
        original_dir = os.getcwd()
        
        logging.info(f"当前工作目录: {original_dir}")
        logging.info(f"PYTHONPATH: {sys.path}")
        
        # 测试 scrapy 命令是否可用
        logging.info("检查 scrapy 命令...")
        
        try:
            which_result = subprocess.run(['which', 'scrapy'], 
                                         capture_output=True, 
                                         text=True,
                                         check=False)
            
            if which_result.returncode == 0:
                scrapy_path = which_result.stdout.strip()
                logging.info(f"找到 scrapy 命令路径: {scrapy_path}")
            else:
                logging.error(f"scrapy 命令不可用: {which_result.stderr}")
                return {
                    'status': '失败',
                    'error': f"找不到 scrapy 命令. 错误输出: {which_result.stderr}"
                }
        except Exception as e:
            logging.error(f"检查 scrapy 命令时出错: {str(e)}")
            return {
                'status': '失败',
                'error': f"检查 scrapy 命令时出错: {str(e)}"
            }
        
        # scrapy_project_dir = Config.SCRAPY_PROJECT_PATH        
        scrapy_project_dir = "/app/flask_web_new2_worker1/spider_manager_4_worker1/scrapy_3"
        logging.info(f"切换到Scrapy项目目录: {scrapy_project_dir}")
        os.chdir(scrapy_project_dir)

        current_dir = os.getcwd()
        logging.info(f"当前工作目录: {current_dir}")

        # 构建 scrapy 命令
        cmd = [scrapy_path, 'crawl', spider_name]

        if spider_name in ['web21spider', 'web22spider', 'web11spider']:
            cmd.extend(['-a', f'docker_id={hostname}'])
            cmd.extend(['-a', f'start_page={start_page}'])
            cmd.extend(['-a', f'end_page={end_page}'])
        

        match spider_name:
            case 'web11spider':
                pass
            case 'web21spider':
                pass
            case 'web22spider':
                pass
            case 'web23spider':
                pass

        
        

        # if start_url:
        #     cmd.extend(['-a', f'start_url={start_url}'])
        # cmd.extend(['-a', f'competitionId=91844'])
        # cmd.extend(['-a', f'task_id={self.request.id}'])
        # cmd.extend(['-a', f'spider={spider_name}'])
        # cmd.extend(['-a', f'ip={ip}'])
        # cmd.extend(['-a', f'docker_id={hostname}'])        
        # cmd.extend(['-a', f'worker_id={self.request.hostname}'])
        
        
        logging.info(f"准备执行命令: {' '.join(cmd)}")
        
        # 执行 scrapy 命令
        logging.info("开始执行爬虫命令...")
        
        process = subprocess.run(cmd, 
                               capture_output=True, 
                               text=True,
                               check=False)
        
        # 记录命令执行结果
        logging.info(f"命令执行完成，返回码: {process.returncode}")
        
        if len(process.stdout) > 0:
            logging.info(f"标准输出前500字符: {process.stdout[:500]}")
        else:
            logging.info("标准输出为空")
            
        if len(process.stderr) > 0:
            logging.info(f"标准错误输出: {process.stderr}")
        else:
            logging.info("标准错误输出为空")
        
        # 返回结果
        if process.returncode != 0:
            return {
                'status': '失败',
                'error': process.stderr,
                'output': process.stdout,
                'returncode': process.returncode
            }
        else:
            return {
                'status': '完成',
                'output': process.stdout
            }
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logging.error(f"任务执行过程中发生异常: {str(e)}")
        logging.error(f"异常调用栈: {tb}")
        
        return {
            'status': '异常',
            'error': str(e),
            'traceback': tb
        }
    finally:
        os.chdir(original_dir)
        logging.info("已经切回原来的目录")
        logging.info("======= 任务结束 =======")