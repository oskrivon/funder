import yaml
import requests
import time
from multiprocessing import Process


class Bot:
    def __init__(self, TOKEN) -> None:
        self.TOKEN = TOKEN
        self.URL = 'https://api.telegram.org/bot'

        self.proxies = ''
        
        self.users = []
        with open('users.yaml') as f:
            self.users = yaml.load(f, Loader=yaml.SafeLoader)
        f.close()

        self.users_list = 'users.yaml'


    def _get_updates(self, offset=0):
        result = requests.get(f'{self.URL}{self.TOKEN}/getUpdates?offset={offset}',
            proxies=self.proxies).json()
        #print(len(result))
        return result['result']
    
    def _message_processing(self, msg, chat_id):
        if msg == '/start':
            self._users_update(chat_id)
        self._send(chat_id, 'ok')
    
    def _send(self, chat_id, text, md_flag=False):
        url = 'https://api.telegram.org/bot' + self.TOKEN + '/sendMessage'

        if md_flag: parse_mode = 'MarkdownV2'
        else: parse_mode = ''

        params = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': parse_mode
        }
        requests.post(url, params= params)
    
    def _send_photo(self, chat_id, path, caption='top by metrics'):
        url = 'https://api.telegram.org/bot' + self.TOKEN + '/sendPhoto'
        files = {'photo': open(path, 'rb')}

        params = {
            'chat_id': chat_id,
            'caption': caption,
            'parse_mode': 'MarkdownV2'
        }
        
        try:
            r = requests.post(url, params= params, files= files)
            print(r)
        except Exception as e:
            print(e)

    def _get_updates(self, offset=0):
        result = requests.get(f'{self.URL}{self.TOKEN}/getUpdates?offset={offset}',
            proxies=self.proxies).json()
        #print(len(result))
        return result['result']

    def broadcast(self, msg):
        for user in self.users:
            self._send(user, str(msg))

    def _users_update(self, chat_id):
        #file = open('users.yaml', 'a+')
        if chat_id in self.users:
            print('user are exist')
        else:
            try:
                self.users.append(chat_id)
                file = open(self.users_list, 'w')
                yaml.dump(self.users, file)
                file.close()
            except Exception as e:
                self._send(1109752742, 'user add error' + str(e))

            try:
                with open('users.yaml') as f:
                    self.users = yaml.load(f, Loader=yaml.SafeLoader)
                    print(self.users)
                f.close()
            except Exception as e:
                self._send(1109752742, 'open file after dump error' + str(e))
    
    def receiving_messages(self):
        self._send(1109752742, '>>> bot is running') # debug
        messages = []

        try:
            update_id = -1
            update_id = self._get_updates()[-1]['update_id']
        except Exception as e:
            self._send(1109752742, 'get last msg error: ' + str(e))
        while True:
            try:
                messages = self._get_updates(update_id)
            except Exception as e:
                self._send(1109752742, 'get updates for msg error: ' + str(e))
            #print(messages)
            try:
                for message in messages:
                    if update_id < message['update_id']:
                        if 'message' in message:
                            chat_id = message['message']['chat']['id']

                            try:
                                msg = message['message']['text']
                                print(f"user id: {chat_id}, message: {msg}")
                            except Exception as e:
                                self._send(1109752742, 'was sender no msg') # debug
                                msg = 'no message'

                            self._message_processing(msg, chat_id)
                            try:
                                update_id = message['update_id']
                            except Exception as e:
                                self._send(1109752742, 'msg is have not update_id') # debug
                        else:
                            update_id = message['update_id']
                            self._message_processing('wtf', chat_id)
            except Exception as e:
                print(e)
                
            time.sleep(5)

    def run(self):
        try:
            th = Process(
                target=self.receiving_messages
            )
            th.daemon = True
            th.start()
            
        except Exception as e:
            print(e)
            self._send(1109752742, 'receiving msg error: ' + str(e))

if __name__ == '__main__':
    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)
    print(cfg)
            
    bot = Bot(cfg['tg_token'])
    bot.run()

    while True:
        time.sleep(1)