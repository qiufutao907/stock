import os
import struct
import numpy as np
import pandas as pd

class TdxReader(object):
    def unpack_records(self, format, data):
        record_struct = struct.Struct(format)
        return (record_struct.unpack_from(data, offset)
                for offset in range(0, len(data), record_struct.size))

    def get_df(self, code_or_file, exchange=None, vipdoc_path='C:\\new_tdx\\vipdoc'):
        tdx_read = TdxBatReader(vipdoc_path)
        code = str(code_or_file)

        if code[0] == '0' or code[0] == '3' or code[0] == '1':
            s = 'sz'
        else:
            s = 'sh'

        df = tdx_read.get_df(code, s)
        df = df.sort_index(ascending=True)
        return df
    
class TdxBatReader(TdxReader):
    """
    读取tdx日线数据
    """

    def __init__(self, vipdoc_path=None):
        self.vipdoc_path = vipdoc_path

    def generate_filename(self, code, exchange):
        if self.vipdoc_path == None:
            print("need a vipdoc path")
        fname = os.path.join(self.vipdoc_path, exchange)
        fname = os.path.join(fname, 'lday')
        fname = os.path.join(fname, '%s%s.day' % (exchange, code))
        return fname

    def get_kline(self, code, exchange):
        fname = self.generate_filename(code, exchange)
        return self.parse_data_by_file(fname)

    def parse_data_by_file(self, fname):
        if not os.path.isfile(fname):
            print('no data, please check path %s' % fname)

        with open(fname, 'rb') as f:
            content = f.read()
            return self.unpack_records('<IIIIIfII', content)
        return []

    def get_df(self, code_or_file, exchange=None):
        if exchange == None:
            return self.get_df_by_file(code_or_file)
        else:
            return self.get_df_by_code(code_or_file, exchange)

    def get_df_by_file(self, fname):
        if not os.path.isfile(fname):
            print('no tdx kline data, please check path %s' % fname)            

        security_type = self.get_security_type(fname)
        if security_type not in self.SECURITY_TYPE:
            print("Unknown security type\n")
            raise NotImplementedError

        coefficient = self.SECURITY_COEFFICIENT[security_type]
        data = [self._df_convert(row, coefficient)
                for row in self.parse_data_by_file(fname)]

        df = pd.DataFrame(data=data, columns=(
            'date', 'open', 'high', 'low', 'close', 'amount', 'volume'))
        df.index = pd.to_datetime(df.date)
        return df[['open', 'high', 'low', 'close', 'amount', 'volume']]

    def get_df_by_code(self, code, exchange):
        fname = self.generate_filename(code, exchange)
        return self.get_df_by_file(fname)

    def _df_convert(self, row, coefficient):
        t_date = str(row[0])
        datestr = t_date[:4] + "-" + t_date[4:6] + "-" + t_date[6:]

        new_row = (
            datestr,
            row[1] * coefficient[0],
            row[2] * coefficient[0],
            row[3] * coefficient[0],
            row[4] * coefficient[0],
            row[5],
            row[6] * coefficient[1]
        )
        return new_row

    def get_security_type(self, fname):
        exchange = str(fname[-12:-10]).lower()
        code_head = fname[-10:-8]
        # print("Exchange:", exchange)
        # print("Code Head:", code_head)

        if exchange == self.SECURITY_EXCHANGE[0]:
            if code_head in ["00", "30", "301"]:
                return "SZ_A_STOCK"
            elif code_head in ["20"]:
                return "SZ_B_STOCK"
            elif code_head in ["39"]:
                return "SZ_INDEX"
            elif code_head in ["15", "16"]:
                return "SZ_FUND"
            elif code_head in ["10", "11", "12", "13", "14"]:
                return "SZ_BOND"
            

        elif exchange == self.SECURITY_EXCHANGE[1]:
            if code_head in ["60","603"]:
                return "SH_A_STOCK"
            elif code_head in ["90"]:
                return "SH_B_STOCK"
            elif code_head in ["00", "88", "99"]:
                return "SH_INDEX"
            elif code_head in ["50", "51"]:
                return "SH_FUND"
            elif code_head in ["01", "10", "11", "12", "13", "14"]:
                return "SH_BOND"
            elif code_head == "58":  # 新添加的类型
                return "SH58"

        print("Unknown security exchange or code head, returning default value 'UNKNOWN'!\n")
        return "UNKNOWN"


    SECURITY_EXCHANGE = ["sz", "sh" ]
    SECURITY_TYPE = ["SH_A_STOCK", "SH_B_STOCK", "SH_INDEX", "SH_FUND",
                     "SH_BOND", "SZ_A_STOCK", "SZ_B_STOCK", "SZ_INDEX", "SZ_FUND", "SZ_BOND","SH58"]
    SECURITY_COEFFICIENT = {"SH_A_STOCK": [0.01, 0.01], "SH_B_STOCK": [0.001, 0.01], "SH_INDEX": [0.01, 1.0], "SH_FUND": [0.001, 1.0], "SH_BOND": [
        0.001, 1.0], "SZ_A_STOCK": [0.01, 0.01], "SZ_B_STOCK": [0.01, 0.01], "SZ_INDEX": [0.01, 1.0], "SZ_FUND": [0.001, 0.01], "SZ_BOND": [0.001, 0.01],"SH58": [0.01, 0.01]}


def convert_to_six_digit_code(lst):
    six_digit_list = [code.zfill(6) for code in lst]
    return six_digit_list

def calculate_duzhan(duzhan_value):
    def RD(N,D=3):
        return np.round(N,D)
    def HHV(S,N):             #HHV(C, 5) 最近5天收盘最高价
        return pd.Series(S).rolling(N).max().values
    def LLV(S,N):             #LLV(C, 5) 最近5天收盘最低价     
        return pd.Series(S).rolling(N).min().values 
    def SMA(S, N, M=1):       #中国式的SMA,至少需要120周期才精确 (雪球180周期)    alpha=1/(1+com)    
        return pd.Series(S).ewm(alpha=M/N,adjust=False).mean().values
    
    # 顶底图
    OPEN = duzhan_value['open']
    CLOSE = duzhan_value['close']
    HIGH = duzhan_value['high']
    LOW = duzhan_value['low']
    VOL = duzhan_value['volume']
    AMOUNT = duzhan_value['amount']
    VAR1 = 1
    VAR5 = LLV(LOW, 75)
    VAR6 = HHV(HIGH, 75)
    VAR7 = (VAR6-VAR5)/100
    VAR4 = (CLOSE-VAR5)/VAR7
    VAR8 = SMA(VAR4, 20, 1)
    VARA = 3*VAR8-2*SMA(VAR8, 15, 1)
    髑战 = RD(((100-VARA)*VAR1),D=2)
    return 髑战[-1]

def read_xlsx_files_in_folder(folder_path):
    result_dict = {}
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file_name)
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name)
                if "code" in df.columns and "name" in df.columns:
                    code_list = df["code"].astype(str).tolist()
                    # 转换为6位数字列表
                    code_list = convert_to_six_digit_code(code_list)
                    result_dict[sheet_name] = dict(zip(code_list, df["name"]))
                    # 将code_list添加到基于sheet_name的对应变量名
                    globals()["code_" + sheet_name.lower() + "_list"] = list(set(code_list))
                else:
                    print(f"处理文件时出错：{file_name} -> 工作表：{sheet_name}。数据格式无效。")
    return result_dict

def DEFEN_stocks(duzhan_value, N1=1.8, N2=1.8):
    def REF(S, N=1):
        return S.shift(N)

    def MAX(S1, S2):
        return np.maximum(S1, S2)

    def ABS(S):
        return np.abs(S)

    # Define HHV function (Assuming it returns the highest value in a series)
    def HHV(S, N):
        return pd.Series(S).rolling(N).max().values

    # Define RD function for rounding (Custom implementation based on context)
    def RD(x, D=2):
        return round(x, D)

    # 获取数据
    OPEN = duzhan_value['open']
    CLOSE = duzhan_value['close']
    HIGH = duzhan_value['high']
    LOW = duzhan_value['low']
    VOL = duzhan_value['volume']    
    X_1 = MAX(MAX(HIGH - LOW, ABS(REF(CLOSE, 1) - HIGH)), ABS(REF(CLOSE, 1) - LOW))
    ATR = X_1.rolling(5).mean()
    DEFEN0 = HHV(CLOSE, 5) - 1.8 * ATR
    DEFEN1 = REF(CLOSE, 1) * 0.929
    if DEFEN0[-1] - DEFEN1[-1] > 0:
        DEFEN = DEFEN0[-1]
    else:
        DEFEN = DEFEN1[-1]
    DEFEN = RD(DEFEN, D=2)

    return DEFEN

def main():
    folder_path = r"C:\Users\Administrator\Projects\Daily_tdx\BK"
    result_dict = read_xlsx_files_in_folder(folder_path)

    tdx_reader = TdxReader()

    # Define the code_lists variable
    code_lists = [code_etf_list, code_qz_list, code_bm_list, code_kj_list,
                  code_zq_list, code_cz_list, code_lhbm_list, code_jgg_list, code_hsa_list]


    category_names = ['ETF', 'QZ', 'BM', 'KJ', 'ZQ', 'CZ', 'LHBM', 'JGG', 'HSA']
    categorized_duzhan_dict = {category: {} for category in category_names}

    for code_list, category_name in zip(code_lists, category_names):
        for code in code_list:
            if code.startswith("688"):
                continue

            exchange = "sz" if code.startswith(("0", "3", "1")) else "sh"
            duzhan_value = tdx_reader.get_df(code, exchange)
            if duzhan_value is None:
                print(f"Unknown security type for stock code: {code}, skipping calculation.")
                continue
            髑战 = calculate_duzhan(duzhan_value)
            防守价 = DEFEN_stocks(duzhan_value)  # Calculate DEFEN

            stock_name = result_dict.get(category_name, {}).get(code, "未知")
            CLOSE = duzhan_value['close']

            categorized_duzhan_dict[category_name][code] = {'name': stock_name, 'duzhan': 髑战, '防守价': 防守价, '收盘价': CLOSE[-1]}

    category_result_list = []

    for category, stocks in categorized_duzhan_dict.items():
        for code, info in stocks.items():
            category_result_list.append({'分类': category, '股票代码': code, '股票名称': info['name'], '髑战值': info['duzhan'], '防守价': info['防守价'],'收盘价': info['收盘价']})   
   
    df_result = pd.DataFrame(category_result_list)
    category_mapping = {
        'ETF': 'ETF类',
        'QZ': '权重类',
        'BM': '白马类',
        'KJ': '科技类',
        'ZQ': '周期类',
        'CZ': '传媒类',
        'LHBM': '板块龙头类',
        'JGG': '机构股类',
        'HSA': '沪深A类'
    }
    df_result['分类'] = df_result['分类'].map(category_mapping)
    summary = df_result.groupby('分类').agg(总股票数量=('股票代码', 'count'), 低风险区域股票数量=('髑战值', lambda x: (x >= 95).sum()))

    print(summary)
    df_result.to_csv('result.csv', index=False, encoding='gbk')

if __name__ == "__main__":
    main()

    








    
