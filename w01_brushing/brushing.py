import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)

order_data = pd.read_csv('order_brush_order.csv')
order_data.head()
order_data['event_time'] = pd.to_datetime(order_data.event_time)


def get_suspicious_buyer(df):
    df.sort_values(by='event_time', inplace=True)

    n = len(df.index)
    is_suspicious = [False for _ in range(n)]

    for i in range(n):
        maxJ -= 1
        userid_set = set()
        for j in range(i, n):
            delta_second = (df['event_time'].iloc[j] - df['event_time'].iloc[i]).total_seconds()
            if delta_second > 3600:
                break
            userid_set.add(df['userid'].iloc[j])
            if j - i + 1 >= len(userid_set) * 3:
                maxJ = j
        for j in range(i, maxJ + 1):
            is_suspicious[j] = True

    brush_df = df.loc[is_suspicious]

    user_count = brush_df.groupby('userid').orderid.count()

    most_suspicious_users = list(user_count[user_count == user_count.max()].index)
    most_suspicious_users.sort()

    res = '&'.join([str(x) for x in most_suspicious_users])
    if res == '':
        res = '0'
    return res


shop_groups = order_data.groupby('shopid')
suspicious_users = []
for shop_id, df in shop_groups:
    suspicious_users.append(get_suspicious_buyer(df))

shop_ids = []
for shop_id, df in shop_groups:
    shop_ids.append(shop_id)

output = pd.DataFrame({'shopid': shop_ids,
                       'userid': suspicious_users})
output.to_csv('submission.csv', index=False)
