import datetime
import time
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
    begin = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - begin
    dates = []
    for i in range(0, delta.days + 1, step):
        date = begin + datetime.timedelta(days=i)
        dates.append(date.strftime("%Y-%m-%d"))
    return dates



def time_interval(begin_time, end_time, interval):
###############################################################################
### This function is for getting all times between begin_date and end_date;
### INPUT : 1) begin_time, e.g : "2022-10-10 12-20-55"
###         2) end_date, e.g : "2022-10-10 22-20-55"
###         3) interval, in seconds
### OUTPUT : list of all time
###############################################################################
    start = time.strptime(begin_time, '%Y-%m-%d %H:%M:%S')
    end = time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    result = []
    current = start
    while current < end:
        result.append(time.strftime('%Y-%m-%d %H:%M:%S', current))
        current = time.mktime(current) + interval
        current = time.localtime(current)
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


