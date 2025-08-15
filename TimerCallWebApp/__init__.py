import azure.functions as func

from .myfunction import testtest

def main(myTimer: func.TimerRequest) -> None:  # ← function.json の name と一致させる


    if myTimer.past_due:
        print("Timer is past due!")
    else:
        testtest() # 外部読み込み確認

    pass