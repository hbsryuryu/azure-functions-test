import azure.functions as func
import datetime
import json
import logging


# ------ローカルファイルパス追加-----
import os
import sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR) # 優先度上げる
# ------ローカルファイル読み込み-----
from action_function.main import main_func

# ------azure function関数-----
app = func.FunctionApp()
# cron_schedule = "0 0 0 1 1 *"
cron_schedule = "0 */5 * * * *" #5分ごと

@app.timer_trigger(schedule=cron_schedule, arg_name="myTimer", run_on_startup=False,use_monitor=False) 
def TimerTrigger1(myTimer: func.TimerRequest) -> None:

    is_late_for_schedule = myTimer.past_due
    
    if is_late_for_schedule:
        logging.info('The timer is past due!') # 指定した時間よりも遅れて実行の場合

    logging.info('Python timer trigger function executed.')
    main_func()