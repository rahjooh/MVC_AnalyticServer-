import conf ,Model.Pyspark_code as spk  ,Model.livy as ssn    #private lib
import textwrap ,requests ,json
from View import visualize_plotly as plotlyit
from Cache import Main as cash

def convert_date(FromMonthNo, ToMonthNo):
    if (FromMonthNo < 10):
        FromMonthNo_Pre="0"+str(FromMonthNo)
    else:
        FromMonthNo_Pre=str(FromMonthNo)


    if(int(FromMonthNo)<=12):
        FromDate='94/'+FromMonthNo_Pre+'/01'
    else:
        FromDate='95/'+"0"+str(int(FromMonthNo)-12)+'/01'


    if (ToMonthNo>12):
        ToMonthNo_Pre="0"+str((int(ToMonthNo)-12))
        ToDate='95/'+ToMonthNo_Pre+'/30'
    else:
        if(ToMonthNo<10):
            ToMonthNo_Pre = "0" + str(ToMonthNo)
        else:
            ToMonthNo_Pre = str(ToMonthNo)
        ToDate = '94/' + ToMonthNo_Pre + '/30'

    return FromDate, ToDate

def main(action,requestid,request_data):
    print("11.Model/model.main           # run related alg on requested action ")
    #some temprory default value
    date_from = "94/09/01"
    date_to = "94/11/30"
    month_number_from = 9
    K = 8
    #extract d_from and d_to
    if "date_from" in request_data and "date_to" in request_data:
        month_number_from = request_data['date_from']
        date_from, date_to = convert_date(int(request_data['date_from']), int(request_data['date_to']))

    if action =='3dclustering' :
        return clustering3D(action,K,month_number_from,date_from,date_to)
    elif action == 'guild_analysis' :
        return guild_analysis(action,K,month_number_from,date_from,date_to)
    elif action =='boxplot_amount' :
        action='mapView'
        return boxplot_amount(action,K,month_number_from,date_from,date_to)
    elif action == 'mapView1':
        return mapView1(action,request_data)



def boxplot_amount(action,K,month_number_from,date_from,date_to):
    code_data1 = {'code': textwrap.dedent(spk.codes[action]).format(month_number_from, "'" + date_from + "'", "'" + date_to + "'", K)}

def guild_analysis(action,K,month_number_from,date_from,date_to):
    code_data1 = {'code': textwrap.dedent(spk.codes[action]).format(month_number_from, "'" + date_from + "'", "'" + date_to + "'", K)}

def clustering3D(action,K,month_number_from,date_from,date_to):
    #get spark code related to action
    code_data1 = {'code': textwrap.dedent("""spark.conf.set("spark.sql.shuffle.partitions",5)
        spark.conf.get("spark.sql.shuffle.partitions")
        spark.conf.set("spark.driver.memory","10g")
        from pyspark.sql import Row
        from pyspark.mllib.clustering import KMeans, KMeansModel
        from pyspark.sql.functions import monotonically_increasing_id
        import pyspark.sql.functions as sf    # for sum agg
        pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqTotal")
        Sp_Df = pqDF.select("Merchantnumber","Amount",((((pqDF.FinancialDate.substr(4,2).cast('int'))+(12*((pqDF.FinancialDate.substr(1,2).cast('int'))-94)))- {0})*30+(pqDF.FinancialDate.substr(7,2).cast('int'))).alias('dayNum')).filter("ProccessCode='000000' AND MessageType='200'AND SuccessOrFailure='S'AND FinancialDate between {1} and {2}")
        Sp_Df= Sp_Df.groupBy(Sp_Df.Merchantnumber,Sp_Df.dayNum).agg(sf.sum(Sp_Df.Amount).alias("TxnSum"),sf.count(Sp_Df.Amount).alias("TxnNo"))
        Sp_Df = Sp_Df.groupBy(Sp_Df.Merchantnumber).agg(sf.sum(Sp_Df.TxnNo).alias("all_transactions"),sf.sum(Sp_Df.TxnSum).alias("sum_amounts"),sf.sum((Sp_Df.TxnNo)/Sp_Df.dayNum).alias("harmonic"))
        Sp_rdd = Sp_Df.rdd
        Sp_Piperdd = Sp_rdd.map(lambda x:(x[1],x[2],x[3]))
        model = KMeans.train(Sp_Piperdd, {3})
        labels =model.predict(Sp_Piperdd)
        labels = labels.map(Row("label")).toDF()   #add schema
        df11 =  Sp_Df.withColumn("id", monotonically_increasing_id())
        df22 =  labels.withColumn("id", monotonically_increasing_id())
        newDF = df11.join(df22, df11.id == df22.id, 'inner').drop(df22.id)
        pandas_df=newDF.toPandas()
        pandas_df.to_json()""").format( month_number_from,"'" + date_from + "'", "'" + date_to + "'",K)}
    #start a new session
    finaldata_df = ssn.get_session(code_data1)
    print(finaldata_df)
    traces = []
    for cluster_num in range(K):
        traces.append(
            finaldata_df[
                finaldata_df["label"] == cluster_num]
        )
    return plotlyit.scatter3d(traces, columns=["all_transactions", "sum_amounts", "harmonic", "label"],
                              title="Kmeans for Real Scale",
                              out_path=conf.PLOT_OUT_DIR)










    #
    os.environ.get("foo")

    map_options = GMapOptions(lat=35.6892, lng=51.3890, map_type='roadmap', zoom=7)
    api_key = os.environ.get('API_KEY')
    api_key = 'AIzaSyCvllwPPFlcF6SF8e2WJBhErb9IYOuqUyk'

    plot = GMapPlot(x_range=DataRange1d(), y_range=DataRange1d(), map_options=map_options, api_key=api_key)
    plot.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool())

    baseline = df['count merchant'].min()
    print(baseline)
    m = df['count merchant'].mean() / zoom

    print(type(df['lat']))
    source = ColumnDataSource(data=dict(lat=df['lat'].values.tolist(),
                                        lon=df['lon'].values.tolist(),
                                        rad=[((i - baseline) / m) + zoom for i in df['count merchant'].values.tolist()]))

    circle = Circle(x="lon", y="lat", size="rad", fill_color='blue', fill_alpha=0.3)

    plot.add_glyph(source, circle)
    output_file('Iran.html')
    show(plot)

def guild_anim(action,K,month_number_from,date_from,date_to):
    print()


def mapView1(plotname,action,data):

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
    #file_html(plot, CDN, plotname)





#main('guilds_heatmap',1,'')
