# # Librerias

# +
import numpy as np
import pandas as pd
import json
import warnings
warnings.filterwarnings('ignore')  
#========================
import pickle
import re
import scrap_socp
import extra_month
import importlib
from extra_month import extrac_json
importlib.reload(scrap_socp)
importlib.reload(extra_month)
from xgboost import plot_importance

import requests, zipfile, io
import requests

import streamlit as st
# -

# # Ubicación json 

path_1="../Data_Json/Raw-15-11-2022-month/releases_2022_noviembre.json"

path_2="../Data_Json/Raw-18-11-2022-month/releases_2022_noviembre.json"

path_3="../Data_Json/Raw-21-11-2022-month/releases_2022_noviembre.json"

path_4="../Data_Json/Raw-22-11-2022-month/releases_2022_noviembre.json"

path_5="../Data_Json/Raw-23-11-2022-month/releases_2022_noviembre.json"

# # Cargamos el Modelo

with open('xgb_model.pkl', 'rb') as f:
    xgb_model= pickle.load(f)


def conu(x):
    try:
        return len(x)
    except:  
        pass


def op_nore(x):
    re_tex=r'Convalidación |convalidación |CONVALIDA |CONVALIDAR |Convalidacion |convalidacion |CONVALIDACION |NO se presentaron preguntas|convalidación,|NO EXISTE CONVALIDACION|CONVALIDAR|CONVALIDACION|convalidación|convalidar|CONVALIDACIÓN'
    try:
        verfi_colum=pd.DataFrame(x)
        if any(verfi_colum.columns=='description') and  any(verfi_colum.columns=='answer') :
            real_ques=[]
            real_ans=[]
            # display(verfi_colum)
            for i,k in zip(verfi_colum['description'],verfi_colum['answer']):
             if len(re.findall(re_tex,i))>0:
                pass
                # print(i,k)
             else:
                real_ques.append(i) 
                if type(k)==str:
                    real_ans.append(k)
                    # display(df)
                else:
                    # print("no respuesta")
                    real_ans.append(k)

            count_no_asn=real_ans.count(None)
            return count_no_asn

        else:
            return len(verfi_colum['description'])
            pass
    except:
        return np.nan


def cer_39(sd,b):
    out_time=[]
    try:
        en_query=b
        # display(sd)
        stf=pd.DataFrame(sd)
        # display(stf)
        stf['date']=pd.to_datetime(stf['date'])

        for i,k in zip(stf['date'],stf['description']):
            if i<=pd.to_datetime(b) :
                # print(i,"-",b)
                pass
            else:
                if len(re.findall(re_tex,k))==1:
                    pass
                else:
                    out_time.append(len(re.findall(re_tex,k)))
                    # print(i,"-",b,"Mayor",len(re.findall(re_tex,k)))
    except:
        pass
    return len(out_time)


def trasf_data(path):
    # with open(path,'r', encoding="utf8") as f:
        # data = json.loads(f.read())
    data = json.load(zj.open(path))
    df = pd.json_normalize(data, record_path =['releases'])
    df=df[df['tender.procurementMethodDetails']=='Subasta Inversa Electrónica']
    for i in df.filter(regex='Date|date', axis=1).columns:
        df[i] = pd.to_datetime(df[i])
    df['NF039']=df.apply(lambda x: cer_39(sd=x['tender.enquiries'],b=x['tender.enquiryPeriod.endDate']),axis=1)

    df['enqui_perio']=(df['tender.enquiryPeriod.endDate']-df['tender.enquiryPeriod.startDate']).dt.days

    df['awar_perio']=(df['tender.awardPeriod.endDate']-df['tender.awardPeriod.startDate']).dt.days

    df['Number_ques']=df['tender.enquiries'].apply(conu)

    df['Pre_No_Respu']=df['tender.enquiries'].apply(op_nore)

    df['n_of_Tend']=df['tender.numberOfTenderers']
    df['Year']=df['tender.tenderPeriod.startDate'].dt.year
    df['Codigo']=df['ocid'].str.extract('ocds-5wno2w-(.*\w*.)-|', expand=True)
    return df


# +
def make_clickable(val):
    """ Create table whit direct click"""
    # target _blank to open new window
    return '<a target="_blank" href="{}">{}</a>'.format(val, val)


#  # Comprobamos mediante Scraping
# df_2_t=trasf_data(path_2)
# df_2=await scrap_socp.ScrapList(df_2_t,code=True)
# -

def predict_df(path,model,ocid=False,scrap=False):
    if scrap==False:
        df=trasf_data(path)
    var_tender=['awar_perio','Pre_No_Respu',
           'enqui_perio', 'Number_ques', 'NF039', 'n_of_Tend']
    test_Ndata=df[var_tender]
    # display(df)
    model.get_booster().feature_names=list( test_Ndata.columns)
    # plot_importance(xgb_model, max_num_features=10)
    predict=model.predict(test_Ndata)
    prob=model.predict_proba(test_Ndata)
    test_Ndata["ocid"]=df['ocid']
    test_Ndata["predict"]=predict
    test_Ndata["proba"]=prob[:, 1]
    if scrap==True:
        test_Ndata["Estado"]=df["Estado del Proceso"]
        test_Ndata["Link"]=df["Link"]
    test_Ndata.set_index('ocid',inplace=True)
    if ocid!=False:
        test_Ndata=test_Ndata[["predict","proba","n_of_Tend",'Pre_No_Respu','awar_perio','enqui_perio', 'Number_ques', 'NF039',]]
        selec_ocid=test_Ndata.loc[[ocid]]
        return selec_ocid
    else:
        test_Ndata=test_Ndata[["predict","proba","n_of_Tend",'Pre_No_Respu','awar_perio','enqui_perio', 'Number_ques', 'NF039']]
        format_df=test_Ndata.style.format({'n_of_Tend': "{:.0f}",'proba': "{:.2%}",'Link': make_clickable},precision=0)\
        .applymap(color_sele, subset=['predict'])\
        .applymap(color_tender, subset=['n_of_Tend'])
        return st.dataframe(format_df)


# +
# df_pred_2=predict_df(path=df_2,model=xgb_model,ocid=False,scrap=True)

# +
def color_sele(val):
    bgcolor = "red" if val > 0 else "white"
    color = "white" if val > 0 else "black"
    return f"color: {color};background-color:{bgcolor}"

def color_desert(val):
    bgcolor = "orange" if val =="Desierta" else "white"
    color = "white" if val == "Desierta" else "black"
    return f"color: {color};background-color:{bgcolor}"

def color_tender(val):
    bgcolor = "yellow" if val ==1 else "white"
    color = "black" if val == 1 else "black"
    return f"color: {color};background-color:{bgcolor}"


# format_df=df_pred.style.format({'n_of_Tend': "{:.0f}",'proba': "{:.2%}",'Link': make_clickable},precision=0).applymap(color_sele, subset=['predict']).applymap(color_desert, subset=['Estado'])\
# .applymap(color_tender, subset=['n_of_Tend'])
# -

st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)

st.write("""<style>
div.css-ocqkz7.e1tzin5v4 {
  display: flex  !important;
  //gap: 10px  !important;
gap: 20px 0px  !important; /* row-gap column gap */
 // row-gap: 10px  !important;
// column-gap: 40px  !important;
//flex-wrap:  wrap  !important;
// flex-grow: 4 !important;
  //justify-content:space-between  !important;
  
  flex-flow: row wrap  !important;
//  display:-webkit-flex;
 // background:#FFD54F;
  padding:20px !important; 
  align-items:flex-end;

}
 </style>""", unsafe_allow_html=True)

st.write("""<style>
div.css-1l269bu.e1tzin5v2{
  background: white;
  padding:20px;
  list-style: none;
  flex: 0 0 30%;
  margin-right: 20px; 
   // background-color: #EEEEEE;
    border: 2px solid #CCCCCC;
    //padding: 5% 5% 5% 10%;
    padding-top: 5%;
    padding-bottom: 5%;
    border-radius: 5px;
    box-shadow: rgba(0, 0, 0, 0.35) 0px 5px 15px;
    justify-content: center;
    text-align: center;
  
}
 </style>""", unsafe_allow_html=True)

bg_col=""" 
div.css-1l269bu.e1tzin5v2:first-child {
background-color:#e63946  
}
"""

font_col=""" 
div.css-1l269bu.e1tzin5v2:first-child{
color:white
}
"""

css_box="""<style>
div.css-1l269bu.e1tzin5v2:first-child {
  padding:20px;
  list-style: none;
  flex: 0 0 30%;
  margin-right: 20px; 
   // background-color: #EEEEEE;
    border: 2px solid #CCCCCC;
    //padding: 5% 5% 5% 10%;
    padding-top: 5%;
    padding-bottom: 5%;
    border-radius: 5px;
    box-shadow: rgba(0, 0, 0, 0.35) 0px 5px 15px;
    justify-content: center;
    text-align: center; 
}
"""

# +
# st.write(f"""<style>
# {bg_col}
# {css_box}
#  </style>""", unsafe_allow_html=True)
# -



center_labes='''
/*center metric label*/
[data-testid="stMetricLabel"] > div:nth-child(1) {
    justify-content: center;
}

/*center metric value*/
[data-testid="stMetricValue"] > div:nth-child(1) {
    justify-content: center;
}

/*center metric delta value*/
div[data-testid="metric-container"] > div[data-testid="stMetricDelta"] > div{
  justify-content: center;
  color: #9E9E9E;
}

/*center metric delta svg*/
[data-testid="stMetricDelta"] > svg {
display:none;
  position: absolute;
  left: 30%;
  -webkit-transform: translateX(-50%);
  -ms-transform: translateX(-50%);
  transform: translateX(-50%);
}

'''

st.title('Deteción de Posibles  Procesos Desiertos (Demo) ')

# +
tipo = st.radio(
    "¿Cual es el tipo de información?",
    ('Link Base de datos', 'Ocid', 'Link Proceso'))

if tipo == 'Link Base de datos':
    urljson_eje='https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/download?type=json&year=2022&month=11&method=Subasta%20Inversa%20Electr%C3%B3nica'
    st.write("Ejemplo: \n",urljson_eje)
    urljson = st.text_input('Ingrese el link de la base de datos por analizar')
    try:
        urlj=urljson
        rj= requests.get(urlj)
        zj = zipfile.ZipFile(io.BytesIO(rj.content))
        hojasj=zj.namelist()
        predict_df(path=hojasj[0],model=xgb_model,ocid=False,scrap=False)
    except:
        pass
elif tipo == 'Ocid':

    
    occi=("ocds-5wno2w-SIE-15BAE-045-2022-54082","ocds-5wno2w-SIE-15BAE-2022-046-54082","ocds-5wno2w-SIE-CELCCS-148B-2022-238940",
          "ocds-5wno2w-SIE-D8D1-E-S-2022-37-15195","ocds-5wno2w-SIE-GADPC-20-2022-54001","ocds-5wno2w-SIEB-GADMSC-014-2022-15065",
         "ocds-5wno2w-SIE-GADPLLCH-02-2022-96227")
    st.subheader('Seleciona un OCID de prueba')
    ocid_code= st.selectbox(
    'Seleciona un OCID de prueba',
       occi,label_visibility="collapsed")
    st.write('El Ocid Selecionado es:', ocid_code)
    # ocids={"Ocid":occi}
    # ejem_ocid=pd.DataFrame(ocids)
    # st.dataframe(ejem_ocid)
    urljson='https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/download?type=json&year=2022&month=11&method=Subasta%20Inversa%20Electr%C3%B3nica'
    urlj=urljson
    rj= requests.get(urlj)
    zj = zipfile.ZipFile(io.BytesIO(rj.content))
    hojasj=zj.namelist()
    try:
        # ocid_code = st.text_input('Ingrese el codigo OCID del Proceso')
        df_ocid=predict_df(path=hojasj[0],model=xgb_model,ocid=ocid_code,scrap=False)
        col1, col2, col3,col4, col5, col6  = st.columns(6)
        if df_ocid["predict"][0]==1:
            col1.metric("Predicción", "Desierto",delta="Tendencia")
            metri_cs=f"""
                     <style>
                        {bg_col}
                        {font_col}
                        {css_box}
                        {center_labes}
                    </style>   
                    """
            st.write(metri_cs, unsafe_allow_html=True)
        else:
            col1.metric("Predicción", "Normal",delta="Tendencia",)
            metri_cs=f"""
                     <style>
                        {css_box}
                        {center_labes}
                    </style>   
                    """
            st.write(metri_cs, unsafe_allow_html=True)
        col2.metric(label="Probabilidad", value=str(round(df_ocid["proba"][0]*100,2))+ "%",delta="de 100 %",)
        if type(df_ocid["n_of_Tend"][0])!=np.float64:
            col3.metric(label="Numero de Oferentes", value="Sin Registro",delta="Oferentes",)
        else:
            col3.metric(label="Numero de Oferentes", value=df_ocid["n_of_Tend"][0] ,delta="Oferentes",
            delta_color="off")
         
        if type(df_ocid["Pre_No_Respu"][0])!=np.float64:
            col4.metric(label="Numero de Oferentes", value="Sin Registro",delta="Oferentes",)
        else:
            col4.metric(label="Preguntas no Respondidas", value=df_ocid["Pre_No_Respu"][0] ,delta="Oferentes",
            delta_color="off")
        
        col5.metric(label="Periodo de Adjudicación", value=str(round(df_ocid["awar_perio"][0])),delta="días")
        col6.metric(label="Periodo de Preguntas", value=str(round(df_ocid["enqui_perio"][0])),delta="días", delta_color="off")
    except:
        pass
 

    
    
    
else:
    st.write("...")

# +
# urljson='https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/download?type=json&year=2022&month=11&method=Subasta%20Inversa%20Electr%C3%B3nica'
# urlj=urljson
# rj= requests.get(urlj)
# zj = zipfile.ZipFile(io.BytesIO(rj.content))
# hojasj=zj.namelist()
# # ocid_code = st.text_input('Ingrese el codigo OCID del Proceso')
# ocid_code="ocds-5wno2w-SIE-GADPLLCH-02-2022-96227"
# predict_df(path=hojasj[0],model=xgb_model,ocid=ocid_code,scrap=False)

# +
# type(df_pred["Pre_No_Respu"][0])
# -

cifr="(?<=/)[^/]*$"
txt="https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/ocds/ocds-5wno2w-SIE-15BAE-045-2022-54082"
x = re.search(cifr, txt)
print(x[0])


# +
# st.dataframe(df_pred_td)  # Same as st.write(df)

# +
# df_pred_2.compare(df_pred_5.loc[df_pred_2.index], keep_equal=True)

# +
# format_df.to_excel(r'predict_22_11_2022.xlsx')
