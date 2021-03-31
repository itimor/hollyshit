import requests
import json


# 武汉华尔街
def to_ding(text):
    token = "b18f740d516e17c03c536639d1d7d01e213aa6a9bcc85d02699c6f884867b233"
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