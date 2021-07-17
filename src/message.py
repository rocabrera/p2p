import json


class Message:


    def __init__(self, content, msg_type, sender, extra_info=None):
        self.content = content
        self.msg_type = msg_type
        self.sender = sender
        self.extra_info = extra_info

    def to_json(self, encoding=None):

        dict_msg = {"content": self.content,
                    "msg_type": self.msg_type,
                    "sender": self.sender,
                    "extra_info": self.extra_info}

        if encoding == "utf-8":
            return json.dumps(dict_msg).encode(encoding)

        return json.dumps(dict_msg)
