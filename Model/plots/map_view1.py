

def mapView1(action,data):

    # map based on merchant count
    # package : bokeh

    mer_df = cash.crm_pdf
    loc_df = cash.loc_pdf

    df_all = loc_df.merge(mer_df, left_on="city name farsi", right_on="CityName")
    df_all = df_all[['city name farsi', 'CityName']]
    df_count_mer = df_all[['city name farsi', 'CityName']].groupby(['city name farsi']).count().reset_index()
    df = loc_df.merge(df_count_mer, left_on="city name farsi", right_on="city name farsi")
    names = df.columns.tolist()
    names[names.index('CityName')] = 'count merchant'
    df.columns = names
    baseline = df['count merchant'].min()
    m = df['count merchant'].mean() / 9




