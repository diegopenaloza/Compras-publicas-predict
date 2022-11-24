# # Importamos Librerias

# +
# Usamos la Libreria Spark para procesar los datos
import findspark
import warnings
import pyspark
from pyspark import SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.pandas as ps
#==============================
import pandas as pd
import time
import numpy as np
import os
import pickle
import re
import yaml
from statistics import mean
#===============================
from importlib import reload
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA


findspark.init()
conf=pyspark.SparkConf().setMaster("local")\
.setAppName("Primer_Spark")\
.setAll([("spark.drive.memory","16g"),
          ("spark.executor.memory","16g") 
        ])

sc=SparkContext.getOrCreate(conf=conf)
spark=SparkSession.builder.appName('Partida_1').getOrCreate()
warnings.filterwarnings('ignore')  



# -

# # Data

# ## Extracción de datos  Globales

path="../Data_Json/Raw-15-11-2022-month/releases_2022_noviembre.json"
def optain_data_json(path,ocid):
    df1 = spark.read.json(path)

    # Extraemos las varaibles relevantes del spark dataframe anidado 
    df1.createOrReplaceGlobalTempView("peopleview")
    def_var=spark.sql("""SELECT    
        -- RELEASES
            releases.ocid[0] ocid,
            releases.date[0] date,
            releases.tag[0] tag,
            releases.bids[0] mc_bids,

            -- PLANNING
                releases.planning.budget.amount.amount[0] PreRefe,

            -- TENDER
                releases.tender.procuringEntity.id[0] in_ent,
                releases.tender.id[0] id,
                releases.tender.description[0] description,
                releases.tender.value.amount[0] Tvalue ,
                releases.tender.procuringEntity.name[0] enti_name,
                releases.tender.procurementMethodDetails[0] PMethoDetails,

                releases.tender.tenderPeriod.startDate[0] TTPSDate,
                releases.tender.tenderPeriod.endDate[0] TTPEDate,
                releases.tender.enquiryPeriod.startDate[0] TEPSDate,
                releases.tender.enquiryPeriod.endDate[0] TEPEDate,
                releases.tender.awardPeriod.startDate[0] TAPSDate,
                releases.tender.awardPeriod.endDate[0] TAPEDate,
                releases.tender.numberOfTenderers[0] n_of_Tend,
                releases.tender.mainProcurementCategory category, 
                releases.tender.enquiries[0] enquiries, 
                releases.tender.enquiries[0].description enq_desc,
                releases.tender.enquiries[0].date enq_fechas,
                releases.tender.enquiries[0].dateAnswered enq_ans_fechas,
                releases.tender.enquiries[0].answer enq_ans,
             -- AWARD
                releases.awards[0].date[0] awardsDate ,
                releases.awards[0].value.amount[0] awardsvalue ,
                releases.awards[0].contractPeriod[0].startDate ACPSDate ,
                releases.awards[0].contractPeriod[0].endDate ACPEDate ,
                releases.awards[0].suppliers[0].id[0] award_id,
            -- Contract
                releases.contracts[0].period.startDate[0] CCPSDate ,
                releases.contracts[0].period.endDate[0] CCPEDate ,
                releases.contracts[0].value.amount[0] ContractValue,
                releases.contracts[0].dateSigned[0] dateSigned,
            -- Auctions
                releases.auctions[0].period.startDate[0] bid_st_Date,
                releases.auctions[0].period.endDate[0] bid_en_Date,
                releases.auctions[0].stages[0].bids[0] bids,
            --Parties
                releases.parties parties,
                YEAR(CAST(releases.tender.tenderPeriod.startDate[0] as date)) year

                FROM global_temp.peopleview"""

                 )

    def_var_ace_fil=def_var.filter((def_var["PMethoDetails"] =='Subasta Inversa Electrónica') )
    
    if ocid!=False:
        def_var_ace_fil_ocid=def_var.filter((def_var["ocid"] ==ocid) )
        df_month_sie=def_var_ace_fil_ocid.toPandas()
        return df_month_sie
    else:
        df_month_sie=def_var_ace_fil.toPandas()
        return df_month_sie


# +
# df=optain_data(path)

# +
# df['enquiries']

# +
# optain_data(df)
# -

# # Generamos otras varaibles

# %%time 
def optain_data(data):
    try:
        for i in data.filter(regex='Date|date', axis=1).columns:
                data[i] = pd.to_datetime(data[i])
    except:
        pass

    try: 
        # Generamos la variable años
        data['Year']=data['TTPSDate'].dt.year
        # Optenemos la diferencia entre el valor de lanzamiento del proceso(Primera fecha de registro del proceso) 
        #y el periodo estimado de adjudicación (Ultima fecha registrada antes de contract)
        data['Dif_Day_Recep_Ofer']=(data['TAPEDate']-data['TTPSDate']).dt.days

        # Periodo de preguntas en dias
        data['enqui_perio']=(data['TEPEDate']-data['TEPSDate']).dt.days

        # Periodo de subastas en dias
        data['audi_perio']=(data['bid_en_Date']-data['bid_st_Date']).dt.total_seconds()/60

        # Periodo de awards en dias
        data['awar_perio']=(data['TAPEDate']-data['TAPSDate']).dt.days
    except:
        pass

    def counter_st(x):
        try:
            tot=list(filter(None, x))
            return len(tot)
        except:
            pass

    data['Number_ques']=data['enquiries'].map(counter_st)    
    data['Num_Pregu']=data['enq_desc'].map(counter_st)
    data['Num_Respu']=data['enq_ans'].map(counter_st)
    data['Pre_No_Respu']=data['Num_Pregu']-data['Num_Respu']

    # Calculamos el promedio de dias en responder una pregunta 
    def fec_di(f,g):
        medi=[]
        f= pd.to_datetime(f)
        g= pd.to_datetime(g)
        try:
            for s,d in zip(g,f):
                di=(s-d).days
                medi.append(di)
            return mean(medi)
        except:
            pass

    data['Mean_Days_Respon']=data.apply(lambda x: fec_di(f = x['enq_fechas'], g = x['enq_ans_fechas']), axis=1)

    # Borramos aquellas columnas con todas sus columnas en nan 
    data.dropna(axis=1, how='all', inplace=True)
    return data


# # Optenemos los red flags

def Nf061(data):
    # Segundo indicador  NF061  en SIE
    """
    Calcula el numero de días entre la fecha de adjudicación y la fecha de firma de Contrato
    """
    try:
        data['nf061']=(data['dateSigned']-\
        data['awardsDate']).dt.total_seconds()/3600
    except:
        pass
    return data


def Nf059(data):
    """
    Calcular la diferencia entre el valor de contrato y el valor de Awards
    """
    try:
        data['nf059']=data['ContractValue']-data['awardsvalue']
    except:
        pass
    return data


def NF039(sd,b):
    try:
        frame_quest=pd.DataFrame(sd,columns=list(sd[0].asDict().keys()))
        frame_quest['id_author'] =frame_quest['author'].apply(lambda x: x[0])
        frame_quest['author'] =frame_quest['author'].apply(lambda x: x[1])
        frame_quest=frame_quest.dropna(subset=['description']).reset_index(drop=True)
   
        frame_quest=frame_quest[~frame_quest['id_author'].str.contains(b)]
        frame_quest=frame_quest[~frame_quest['description'].str.contains('Convalidación|convalidación|CONVALIDA|CONVALIDAR\
                                                                         |Convalidacion|convalidacion|CONVALIDACION')]

        find_nan=frame_quest['answer'].isnull().values.any()

        if find_nan==True:
            non_ans=frame_quest['answer'].isnull().sum()
            return non_ans
        else:
            return 0
    except:
        pass


def NF018(x):
    try:
        return len(x)
    except:
        pass


def Nf030(sd,b,c):
    try:
        frame_quest=pd.DataFrame(sd,columns=list(sd[0].asDict().keys()))
        frame_quest['date'] = pd.to_datetime(frame_quest['date'])
        frame_quest['id_author'] =frame_quest['tenderers'].apply(lambda x: x[0][0])
        frame_quest['author'] =frame_quest['tenderers'].apply(lambda x: x[0][1])
        frame_quest['value']=frame_quest['value'].apply(lambda x: x[0])
        frame_quest=frame_quest.drop(columns=['tenderers'])
        # frame_quest=frame_quest[frame_quest['date']>b]
        frame_quest=frame_quest[frame_quest['date']>c]
        return len(frame_quest)
    except:
        pass


def red_flags_Contrac(data):
    Nf061(data=data)
    Nf059(data=data)
    data['NF039']=data.apply(lambda x: NF039(sd=x['enquiries'],b=x['in_ent']),axis=1)
    data['NF018']=data['bids'].apply(NF018)
    data['Nf030']=data.apply(lambda x: Nf030(sd=x['bids'],
                                                  b=x['bid_st_Date'],
                                                  c=x['bid_en_Date']),axis=1)
    return data


def extrac_json(path,ocid=False):
    df_month_sie=optain_data_json(path,ocid)
    df_month_sie_add=optain_data(df_month_sie)
    df_convrt=red_flags_Contrac(df_month_sie_add)
    return df_convrt

# +
# extrac_json(path)
# -

# ---
