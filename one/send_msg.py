import requests
import json


# 万能的真主
def to_ding(text):
    token = "807f980f2c2c278c03e881da9c684d943c47e7a9f1ffc8da567f8e1b377a606e"
    headers = {'Content-Type': 'application/json;charset=utf-8'}  # 请求头
    api_url = f"https://oapi.dingtalk.com/robot/send?access_token={token}"
    json_text = {
        "msgtype": "text",  # 信息格式
        "text": {
            "content": text
        }
    }
    r = requests.post(api_url, json.dumps(json_text), headers=headers)
    print(r.json())
