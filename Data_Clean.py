import datetime
import time
import numpy as np
import copy
###############################################################################
###############################################################################


def TimeStamp_to_NormalTime(timestamp, more_precise = False):
###############################################################################
### This function is for transform timestamp to time;
### INPUT : 1) timestamp, Int
###         2) precision, the precision of time, default in second, if need more preciser
###                       data, change it to True
### OUTPUT : Normal time
###############################################################################
    Length = len(str(timestamp))  
    if Length < 10 :
        raise ValueError("This is not time stamp")
    else:
        
        dt_object = datetime.datetime.fromtimestamp(timestamp / 10 ** (Length - 10))
        if more_precise:
            return dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            return dt_object.strftime("%Y-%m-%d %H:%M:%S")


def Normal_to_Timestamp(local_time):
###############################################################################
### This function is for transform time to timestamp;
### INPUT : 1) time, e.g: "2022-10-10" or "2022-10-10 10:25:36"
### OUTPUT : timestamp
###############################################################################
    if len(local_time) == 10:
        dt = datetime.datetime.strptime(local_time, '%Y-%m-%d')
        return (dt - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1)
    elif len(local_time) == 19:
        dt = datetime.datetime.strptime(local_time, '%Y-%m-%d %H:%M:%S')
        return (dt - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1)
    else:
        dt = datetime.datetime.strptime(local_time, '%Y-%m-%d %H:%M:%S.%f')
        return (dt - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1)


def get_date_range(begin_date, end_date, step=1):
###############################################################################
### This function is for getting All dates between begin_date and end_date;
### INPUT : 1) step, the number of days between adjacent dates, default 1; INT
###         2) begin_date, e.g : "2022-10-10"
###         3) end_date, e.g : "2022-10-10"
### OUTPUT : Normal time
###############################################################################
    dates = [(datetime.datetime.strptime(begin_date, "%Y-%m-%d") + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
             for i in range(0, (datetime.datetime.strptime(end_date, "%Y-%m-%d") - datetime.datetime.strptime(begin_date, "%Y-%m-%d")).days + 1, step)]
    return dates



def time_interval(begin_time, end_time, interval, istimestamp = False):
###############################################################################
### This function is for getting all times between begin_date and end_date;
### INPUT : 1) begin_time, e.g : "2022-10-10 12-20-55"
###         2) end_date, e.g : "2022-10-10 22-20-55"
###         3) interval, in seconds
###         4) istimestapm, if true return timestamp
### OUTPUT : list of all time
###############################################################################
    start = datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    result = []
    current = start
    while current < end:
        result.append(current.strftime('%Y-%m-%d %H:%M:%S'))
        current += datetime.timedelta(seconds=interval)

    if istimestamp:
        result = [int(time.mktime(datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S').timetuple())) for dt in result]
    return result


def Choose_Period_Data(df, begin_time, end_time):
###############################################################################
### This function is for getting data from specified period;
### INPUT : 1) df, dataframe with timestamp
###         2) begin_time, e.g : "2022-10-10 22-20-55" (other format accepted), included
###         3) end_time, e.g : "2022-10-10 22-20-55"(other format accepted), included
### OUTPUT : dataframe
###############################################################################   
    begin_time, end_time = Normal_to_Timestamp(begin_time), Normal_to_Timestamp(end_time)
    df = df[(df['timestamp'] >= begin_time) & (df['timestamp'] <= end_time)]
    return df

def Date_Addtion(date, date_type, num):
    ###############################################################################
    ### The purpose of this equation is to add or subtract time from the specified date;
    ### INPUT : 1) date, target date, in "%Y-%m-%d" or '%Y-%m-%d %H:%M:%S' format
    ###         2) date_tyoe, The type of time you want to increase, choose from
    ###            "day", "hour", "second"
    ###         3) num, the amount of time you want to add
    ### OUTPUT : Time after adding the specified amount of time
    ###############################################################################
    if date_type == "day":
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
        delta = datetime.timedelta(days=num)
        new_date_obj = date_obj + delta
        return new_date_obj.strftime("%Y-%m-%d")
    elif date_type == "hour":
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        delta = datetime.timedelta(hours=num)
        new_date_obj = date_obj + delta
        return new_date_obj.strftime('%Y-%m-%d %H:%M:%S')
    elif date_type == "second":
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        delta = datetime.timedelta(seconds=num)
        new_date_obj = date_obj + delta
        return new_date_obj.strftime('%Y-%m-%d %H:%M:%S')


def Timestamp_List(start_time, end_time, interval):
    ###############################################################################
    ### The purpose of this function is to determine the time stamp of the given frequency
    ### between outputs according to the start and end time of the input.;
    ### INPUT : 1) start_time, in 16 timestamp format
    ###         2) end_time, in 16 timestamp format
    ###         3) interval, in seconds
    ### OUTPUT : timestamp list
    ###############################################################################
    second_start = start_time // 1000000
    second_end = end_time // 1000000
    timestamps = []
    for second in range(second_start, second_end, interval):
        timestamps.append(second)
    return timestamps

def Clean_Outlier(data, std_num=3, standarization = True):
    series = copy.deepcopy(data)
    mean = np.mean(series)
    std = np.std(series)
    min_value = mean - std_num * std
    max_value = mean + std_num * std
    if len(series[series < min_value]) > 0:
        series.loc[series < min_value] = min_value
    if len(series[series > max_value]) > 0:
        series.loc[series > max_value] = max_value

    if standarization:
        mean = np.mean(series)
        std = np.std(series)
        series = (series - mean) / std
    return series
