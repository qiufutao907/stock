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
            if code_head in ["00", "30"]:
                return "SZ_A_STOCK"
            elif code_head in ["20"]:
                return "SZ_B_STOCK"
            elif code_head in ["39"]:
                return "SZ_INDEX"
            elif code_head in ["15", "16"]:
                return "SZ_FUND"
            elif code_head in ["10", "11", "12", "13", "14"]:
                return "SZ_BOND"
            elif code_head == "58":  # 新添加的类型
                return "SH58"

        elif exchange == self.SECURITY_EXCHANGE[1]:
            if code_head in ["60"]:
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
                     "SH_BOND", "SZ_A_STOCK", "SZ_B_STOCK", "SZ_INDEX", "SZ_FUND", "SZ_BOND","SH58","SH68"]
    SECURITY_COEFFICIENT = {"SH_A_STOCK": [0.01, 0.01], "SH_B_STOCK": [0.001, 0.01], "SH_INDEX": [0.01, 1.0], "SH_FUND": [0.001, 1.0], "SH_BOND": [
        0.001, 1.0], "SZ_A_STOCK": [0.01, 0.01], "SZ_B_STOCK": [0.01, 0.01], "SZ_INDEX": [0.01, 1.0], "SZ_FUND": [0.001, 0.01], "SZ_BOND": [0.001, 0.01],"SH58": [0.01, 0.01],"SH68": [0.01, 0.01]}


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

import pandas as pd

def main():
    folder_path = r"C:\Users\Administrator\Projects\Daily_tdx\BK"
    result_dict = read_xlsx_files_in_folder(folder_path)

    # 创建一个字典来存储股票代码和对应的髑战值
    duzhan_dict = {}

    tdx_reader = TdxReader()

    # 多个代码列表
    code_lists = [code_etf_list, code_qz_list, code_bm_list, code_kj_list,
                  code_zq_list, code_cz_list, code_lhbm_list, code_jgg_list]

    # 多个代码列表对应的分类名
    category_names = ['ETF', 'QZ', 'BM', 'KJ', 'ZQ', 'CZ', 'LHBM', 'JGG']
    # 创建一个字典来存储按分类分组的髑战值
    categorized_duzhan_dict = {category: {} for category in category_names}

    # 创建一个字典来存储髑战值大于等于95和小于5的个数及对应的股票代码和名称
    count_gt_95_dict = {category: 0 for category in category_names}
    count_lt_5_dict = {category: 0 for category in category_names}
    stocks_gt_95_dict = {category: [] for category in category_names}
    stocks_lt_5_dict = {category: [] for category in category_names}

    # 遍历多个代码列表和分类名
    for code_list, category_name in zip(code_lists, category_names):
        # 遍历股票代码列表并计算髑战值，然后存储到字典中
        for code in code_list:
            # 过滤掉以688开头的股票代码
            if code.startswith("688"):
                continue

            exchange = "sz" if code.startswith(("0", "3", "1")) else "sh"
            duzhan_value = tdx_reader.get_df(code, exchange)
            if duzhan_value is None:
                print(f"Unknown security type for stock code: {code}, skipping calculation.")
                continue
            髑战 = calculate_duzhan(duzhan_value)

            # 获取股票名称，如果result_dict中没有对应的分类名或股票代码，使用"未知"作为默认值
            stock_name = result_dict.get(category_name, {}).get(code, "未知")

            # 将结果存储到duzhan_dict中
            duzhan_dict[code] = {'name': stock_name, 'duzhan': 髑战}
            # 将结果存储到按分类分组的字典中
            categorized_duzhan_dict[category_name][code] = {'name': stock_name, 'duzhan': 髑战}

            # 判断髑战值是否大于等于95或小于5，并进行计数和记录对应的股票代码和名称
            if 髑战 >= 95:
                count_gt_95_dict[category_name] += 1
                stocks_gt_95_dict[category_name].append((code, stock_name, 髑战))
            elif 髑战 < 5:
                count_lt_5_dict[category_name] += 1
                stocks_lt_5_dict[category_name].append((code, stock_name, 髑战))

    print("\n按分类分组的髑战值：")
    print("------------------------")
    for category, stocks in categorized_duzhan_dict.items():
        print(f"\n分类：{category}")
        for code, info in stocks.items():
            print(f"股票代码: {code}, 股票名称: {info['name']}, 髑战值: {info['duzhan']}")

    print("\n髑战值大于等于95的个数及对应的股票代码和名称：")
    for category, count in count_gt_95_dict.items():
        print(f"{category}: {count}")
        for stock_info in stocks_gt_95_dict[category]:
            print(f"股票代码: {stock_info[0]}, 股票名称: {stock_info[1]}, 髑战值: {stock_info[2]}")

    print("\n髑战值小于5的个数及对应的股票代码和名称：")
    for category, count in count_lt_5_dict.items():
        print(f"{category}: {count}")
        for stock_info in stocks_lt_5_dict[category]:
            print(f"股票代码: {stock_info[0]}, 股票名称: {stock_info[1]}, 髑战值: {stock_info[2]}")



if __name__ == "__main__":
    main()









    