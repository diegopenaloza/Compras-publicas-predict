import asyncio
from pyppeteer import launch
import re
import time
import pandas as pd
import numpy as np
import math


# Creamos una función para realizar usar en los datos extraidos de la web
def remove_tags(text):
    TAG_RE = re.compile(r'<[^>]+>')
    """ Nos permite obtener solo los textos estraidos de la web """
    TAG_RE.sub('', text)

    return TAG_RE.sub('', text)


def texto(texto):
    lista=[x for x in remove_tags(texto).splitlines()]
    lista[-1]=lista[-1].replace("&nbsp;", "")
    op_ur=re.findall(r'(?<=href=").*(?=,">)', texto)[0]
    url_Sc=["https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/PC/"+op_ur]
    # dfn=fech[i].replace("&nbsp;", "")
    return url_Sc+lista[1:]


async def ScrapList(dataset,code=True):
    
    if code ==False:
        dataset['Codigo']=dataset['ocid'].str.extract('ocds-5wno2w-(.*\w*.)-|', expand=True)
    else:
        pass
    codigo=list(dataset['Codigo'])
    year=list(dataset['Year'])
    ocid=list(dataset['ocid'])
    browser = await launch(headless=False)
    page = await browser.newPage()
    await page.goto('https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/PC/buscarProceso.cpe')
    porcesos=[]
    recolect_2 = await page.evaluate('''(codigo,year) => { 
    
    var codigo=codigo
    var year=year
    var Datos = [];
    var Datos_nan = [];
    for(var i=0;i<codigo.length;i++){
    
        document.getElementById("txtCodigoProceso").value= codigo[i]
        var inicio= new Date(year[i], 1-1, 01)
        var final = new Date(year[i], 12-1, 31) 
        function tran(date){
                    var year = date.getFullYear();
                    var  month = String(date.getMonth() +1).padStart(2, '0');
                    var  day = String(date.getDate()).padStart(2, '0');
                    var  joined = [year, month, day].join('-');
                return joined
            }  

        for (var d = inicio; d <= final; d.setMonth(d.getMonth() + 6)) {
                    var dia_ini= new Date(d)
                    var dia_fina= new Date(d).setMonth(d.getMonth() + 6);
                    var dia_fina_1= new Date(dia_fina).setDate(new Date(dia_fina).getDate() - 1);
                    var est1= new Date(dia_ini)
                    var est2=new Date(dia_fina_1)
                    var fechaIni = tran(est1)
                    var fechaFin =tran(est2)
                    var feini = document.getElementById("f_inicio");    
                    var feFin= document.getElementById("f_fin");  
                    feini.value = fechaIni;
                    feFin.value = fechaFin;
                    botonBuscar()
                    presentarProcesos(0);

                    var existe=(document.getElementById("divProcesos").getElementsByTagName("tr")).length

                    if (existe !=0) {
                      console.log("Existe")
                      var dato= document.getElementById("divProcesos").getElementsByTagName("tr")[1].innerHTML;
                      Datos.push(dato);
                      console.log(dato)
                      Datos_nan.push(codigo[i]);
                      break; 
                    }else {
                      console.log("No existe")
                        }
                    }
            }
            
         return {Datos,Datos_nan}; 

        }''',codigo,year)

    await browser.close()
    
    recolect_3=recolect_2['Datos']
    colum_name=['Link','Codigo','Entidad Contratante','Objeto del Proceso','Estado del Proceso','Provincia/Cantón','Presupuesto','date']
    texto_2=list(map(texto,recolect_3))
    opta=pd.DataFrame(texto_2)
    try:
        opta=opta.drop([8], axis=1)
        opta=opta.drop([9], axis=1)
    except: 
        pass
    opta.columns = colum_name
    opta['Codigo']=opta['Codigo'].str.replace(' ','')
    opta.set_index('Codigo',inplace=True)
    df_in_ocid=dataset.set_index('Codigo')
    # display(df_in_ocid)
    merge_new_data=pd.concat([ df_in_ocid, opta], axis=1,join="inner")
    
    return merge_new_data


def optain_scrap_table(recolect_2,df_total):
    recolect_3=recolect_2['Datos']
    colum_name=['Link','Codigo','Entidad Contratante','Objeto del Proceso','Estado del Proceso','Provincia/Cantón','Presupuesto','date']
    texto_2=list(map(texto,recolect_3))
    opta=pd.DataFrame(texto_2)
    try:
        opta=opta.drop([8], axis=1)
        opta=opta.drop([9], axis=1)
    except: 
        pass
    opta.columns = colum_name
    opta['Codigo']=opta['Codigo'].str.replace(' ','')
    opta.set_index('Codigo',inplace=True)
    df_in_ocid=df_total.set_index('Codigo')
    # display(df_in_ocid)
    merge_new_data=pd.concat([ df_in_ocid, opta], axis=1,join="inner")
    # merge_new_data=pd.merge(df_flags_enti, opta, on=["Codigo"])
    return  merge_new_data


def serch_indata(outlider_data):
    # outlider_selec=outlider_data[outlider_data['pred']==1]
    outlider_selec=outlider_data
    df_in_ocid=df_flags_enti[['ocid','Codigo','Year']].set_index('ocid')
    concat=pd.concat([df_in_ocid, outlider_selec], axis=1,join="inner")
    concat=concat.reset_index()
    codi=list(concat['Codigo'])
    ye=list(concat['Year'])
    Oci=list(concat['ocid'])
    return codi,ye,Oci,concat


def opt_new(df_flags_enti):
    df_flags_enti['Codigo']=df_flags_enti['ocid'].str.extract('ocds-5wno2w-(.*\w*.)-|', expand=True)
    codi=list(df_flags_enti['Codigo'])
    ye=list(df_flags_enti['Year'])
    Oci=list(df_flags_enti['ocid'])

