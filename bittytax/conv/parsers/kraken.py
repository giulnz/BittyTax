# -*- coding: utf-8 -*-

# (c) Nano Nano Ltd 2020



from decimal import Decimal



from ..out_record import TransactionOutRecord

from ..dataparser import DataParser

from ..exceptions import UnexpectedTypeError, UnexpectedTradingPairError



WALLET = "Kraken"



QUOTE_ASSETS = ['AUD', 'CAD', 'CHF', 'DAI', 'DOT', 'ETH', 'EUR', 'GBP', 'JPY', 'USD',

                'USDC', 'USDT', 'XBT', 'XETH', 'XXBT', 'ZAUD', 'ZCAD', 'ZEUR', 'ZGBP', 'ZJPY',

                'ZUSD']



ALT_ASSETS = {'KFEE': 'FEE', 'XETC': 'ETC', 'XETH': 'ETH', 'XLTC': 'LTC', 'XMLN': 'MLN',

              'XREP': 'REP', 'XXBT': 'XBT', 'XXDG': 'XDG', 'XXLM': 'XLM', 'XXMR': 'XMR',

              'XXRP': 'XRP', 'XZEC': 'ZEC', 'ZAUD': 'AUD', 'ZCAD': 'CAD', 'ZEUR': 'EUR',

              'ZGBP': 'GBP', 'ZJPY': 'JPY', 'ZUSD': 'USD'}



ASSETS_2CHARS = ['SC']



def parse_kraken_all(data_row, _parser, **_kwargs):

    row_dict = data_row.row_dict

    data_row.timestamp = DataParser.parse_timestamp(row_dict['time'])



    if row_dict['type'] == "deposit" and row_dict['txid'] != "":

        # Check for txid to filter failed transactions

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,

                                                 data_row.timestamp,

                                                 buy_quantity=row_dict['amount'],

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 fee_quantity=row_dict['fee'],

                                                 fee_asset=normalise_asset(row_dict['asset']),

                                                 note=row_dict['txid'],

                                                 wallet=WALLET)

    elif row_dict['type'] == "withdrawal" and row_dict['txid'] != "":

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,

                                                 data_row.timestamp,

                                                 sell_quantity=abs(Decimal(row_dict['amount'])),

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 fee_quantity=row_dict['fee'],

                                                 fee_asset=normalise_asset(row_dict['asset']),

                                                 note=row_dict['txid'],

                                                 wallet=WALLET)

    elif row_dict['type'] == "adjustment" and row_dict['amount'] >= '0':

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_GIFT_RECEIVED,

                                                 data_row.timestamp,

                                                 buy_quantity=row_dict['amount'],

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 buy_value='0',

                                                 note='adjustment '+row_dict['refid']+' '+row_dict['txid'],                                              

                                                 wallet=WALLET)

    elif row_dict['type'] == "adjustment" and row_dict['amount'] < '0':                                                  

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_GIFT_SENT,

                                                 data_row.timestamp,

                                                 sell_quantity=abs(Decimal(row_dict['amount'])),

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 sell_value='0',

                                                 note='adjustment '+row_dict['refid']+' '+row_dict['txid'],                                              

                                                 wallet=WALLET)                                               

    elif row_dict['type'] == "transfer" and row_dict['amount'] >= '0':

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_GIFT_RECEIVED,

                                                 data_row.timestamp,

                                                 buy_quantity=row_dict['amount'],

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 buy_value='0',

                                                 note='transfer '+row_dict['refid']+' '+row_dict['txid'],                                              

                                                 wallet=WALLET)

    elif row_dict['type'] == "transfer" and row_dict['amount'] < '0':                                                  

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_GIFT_SENT,

                                                 data_row.timestamp,

                                                 sell_quantity=abs(Decimal(row_dict['amount'])),

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 sell_value='0',

                                                 note='transfer '+row_dict['refid']+' '+row_dict['txid'],                                              

                                                 wallet=WALLET) 

    elif row_dict['type'] == "rollover":

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,

                                                 data_row.timestamp,

                                                 buy_quantity='0',

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 sell_quantity='0',                                            

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 fee_quantity=row_dict['fee'],

                                                 fee_asset=normalise_asset(row_dict['asset']),

                                                 note='rollover '+row_dict['refid']+' '+row_dict['txid'],

                                                 wallet=WALLET)                                               

    elif row_dict['type'] == "settled" and row_dict['amount'] >= '0':

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,

                                                 data_row.timestamp,

                                                 buy_quantity=row_dict['amount'],

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 sell_quantity='0',                                            

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 note='settled '+row_dict['refid']+' '+row_dict['txid'],

                                                 wallet=WALLET)

    elif row_dict['type'] == "settled" and row_dict['amount'] < '0': 

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,

                                                 data_row.timestamp,

                                                 buy_quantity='0',

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 sell_quantity=abs(Decimal(row_dict['amount'])),                                            

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 note='settled '+row_dict['refid']+' '+row_dict['txid'],

                                                 wallet=WALLET)

    elif row_dict['type'] == "margin" and row_dict['amount'] >= '0': 

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,

                                                 data_row.timestamp,

                                                 buy_quantity=row_dict['amount'],

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 sell_quantity='0',                                            

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 fee_quantity=row_dict['fee'],

                                                 fee_asset=normalise_asset(row_dict['asset']),

                                                 note=row_dict['txid']+' '+row_dict['refid']+' '+row_dict['txid'],

                                                 wallet=WALLET)

    elif row_dict['type'] == "margin" and row_dict['amount'] < '0':

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,

                                                 data_row.timestamp,

                                                 buy_quantity='0',

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 sell_quantity=abs(Decimal(row_dict['amount'])),                                            

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 fee_quantity=row_dict['fee'],

                                                 fee_asset=normalise_asset(row_dict['asset']),

                                                 note=row_dict['txid']+' '+row_dict['refid']+' '+row_dict['txid'],

                                                 wallet=WALLET) 

    elif row_dict['type'] == "trade" and row_dict['amount'] >= '0': 

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,

                                                 data_row.timestamp,

                                                 buy_quantity=row_dict['amount'],

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 sell_quantity='0',                                            

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 fee_quantity=row_dict['fee'],

                                                 fee_asset=normalise_asset(row_dict['asset']),

                                                 note=row_dict['refid']+' '+row_dict['txid'],

                                                 wallet=WALLET)

    elif row_dict['type'] == "trade" and row_dict['amount'] < '0':

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,

                                                 data_row.timestamp,

                                                 buy_quantity='0',

                                                 buy_asset=normalise_asset(row_dict['asset']),

                                                 sell_quantity=abs(Decimal(row_dict['amount'])),                                            

                                                 sell_asset=normalise_asset(row_dict['asset']),

                                                 fee_quantity=row_dict['fee'],

                                                 fee_asset=normalise_asset(row_dict['asset']),

                                                 note=row_dict['refid']+' '+row_dict['txid'],

                                                 wallet=WALLET)

                                                                  



def split_trading_pair(trading_pair):

    for quote_asset in sorted(QUOTE_ASSETS, reverse=True):

        if trading_pair.endswith(quote_asset) and (len(trading_pair)-len(quote_asset) >= 3 \

                or trading_pair[:2] in ASSETS_2CHARS):

            return trading_pair[:-len(quote_asset)], quote_asset



    return None, None



def normalise_asset(asset):

    if asset in ALT_ASSETS:

        asset = ALT_ASSETS.get(asset)



    if asset == "XBT":

        return "BTC"

    return asset



DataParser(DataParser.TYPE_EXCHANGE,

           "Kraken LEDGER",

           ['txid', 'refid', 'time', 'type', 'subtype', 'aclass', 'asset', 'amount', 'fee',

            'balance'],

           worksheet_name="Kraken D,W",

           row_handler=parse_kraken_all)

