import asyncio, threading, websockets, json, datetime
from datetime import datetime

class ticker:

    def __init__(self, symbols='btcusdt', verbose=False):
        if isinstance(symbols, str):
            symbols = [symbols]
        self.loop = asyncio.get_event_loop()
        self.symbols = symbols
        self.states = {}
        for symbol in symbols:
            self.states[symbol] = {'price': 0, 'volume': 0}
        self.verbose = verbose
        if verbose:
            self.saves = 0

    async def connect(self):
        self.connection = await websockets.connect("wss://stream.binance.com/stream", origin="https://www.binance.com")
        await self.doMessages()
        await asyncio.Future()

    async def doMessages(self):
        await self.subscribe()
        while True:
            message = json.loads(await self.connection.recv())
            self.doMessage(message)

    def doMessage(self, data):
        try:
            ticker = data['stream'].split('@')[0]
        except Exception as err:
            print(err)
            return
        self.states[ticker] = {'price': data['data']['p'], 'volume': data['data']['q'], 'sell': data['data']['m']}
        if self.verbose:
            self.saves += 1

    async def subscribe(self):
        id = 1
        for symbol in self.symbols:
            await self.connection.send('{"method":"SUBSCRIBE","params":["'+symbol+'@aggTrade"],"id":'+str(id)+'}')
            id += 2

    async def giveAnUpdate(self):
        while True:
            await asyncio.sleep(5)
            print("{}: Watching {} tickers → received {} updates".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), len(self.symbols), self.saves))
            self.saves = 0

    def start(self):
        self.loop = asyncio.new_event_loop()
        def _start(loop):
            asyncio.set_event_loop(loop)
            self.task = loop.create_task(self.connect())
            if self.verbose:
                self.updateTask = loop.create_task(self.giveAnUpdate())
            loop.run_forever()
        t = threading.Thread(target=_start, args=(self.loop,))
        t.start()
        self.thread = t

    def stop(self):
        self.task.cancel()
        if self.verbose:
            self.updateTask.cancel()
        self.loop.stop()
        self.thread.join()
        self.db.close()
