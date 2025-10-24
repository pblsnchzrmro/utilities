import logging
import pandas as pd
import numpy as np

def frontur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys()))["VISITANTES"].sum().reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def egatur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "PERNOCTACIONES": "sum",
        "GASTO_TOTAL": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def etr(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "PERNOCTACIONES": "sum",
        "GASTO_TOTAL": "sum",
        "VIAJES": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def aena_aerolineas(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "PASAJEROS_POR_AEROLINEA": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def aena_destinos(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "PASAJEROS_POR_DESTINO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def eoh_categoria(df, fields):
    def custom_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.sum()

    def custom_mean_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.mean()

    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": custom_agg,
        "PERNOCTACIONES": custom_agg,
        "ESTANCIA_MEDIA": custom_mean_agg,
        "ESTABLECIMIENTOS_ESTIMADOS": custom_agg,
        "HABITACIONES_ESTIMADAS": custom_agg,
        "PLAZAS_ESTIMADAS": custom_agg,
        "GRADO_OCUPA_PLAZAS": custom_mean_agg,
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": custom_mean_agg,
        "GRADO_OCUPA_POR_HABITACIONES": custom_mean_agg,
        "PERSONAL_EMPLEADO": custom_agg
    }).reset_index()
    
    fields_to_cast = ["VIAJEROS", "PERNOCTACIONES", "ESTABLECIMIENTOS_ESTIMADOS", "HABITACIONES_ESTIMADAS", "PLAZAS_ESTIMADAS", "PERSONAL_EMPLEADO"]
    df[fields_to_cast] = df[fields_to_cast].astype('Int64')

    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]
                
    return df

def eoh_prov(df, fields):
    def custom_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.sum()

    def custom_mean_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.mean()

    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": custom_agg,
        "PERNOCTACIONES": custom_agg,
        "ESTANCIA_MEDIA": custom_mean_agg,
        "ESTABLECIMIENTOS_ESTIMADOS": custom_agg,
        "HABITACIONES_ESTIMADAS": custom_agg,
        "PLAZAS_ESTIMADAS": custom_agg,
        "GRADO_OCUPA_PLAZAS": custom_mean_agg,
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": custom_mean_agg,
        "GRADO_OCUPA_POR_HABITACIONES": custom_mean_agg,
        "PERSONAL_EMPLEADO": custom_agg
    }).reset_index()
    
    fields_to_cast = ["VIAJEROS", "PERNOCTACIONES", "ESTABLECIMIENTOS_ESTIMADOS", "HABITACIONES_ESTIMADAS", "PLAZAS_ESTIMADAS", "PERSONAL_EMPLEADO"]
    df[fields_to_cast] = df[fields_to_cast].astype('Int64')
        
    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]
                
    return df


def eoh_ccaa(df, fields):
    def custom_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.sum()

    def custom_mean_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.mean()

    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": custom_agg,
        "PERNOCTACIONES": custom_agg,
        "ESTANCIA_MEDIA": custom_mean_agg,
        "ESTABLECIMIENTOS_ESTIMADOS": custom_agg,
        "HABITACIONES_ESTIMADAS": custom_agg,
        "PLAZAS_ESTIMADAS": custom_agg,
        "GRADO_OCUPA_PLAZAS": custom_mean_agg,
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": custom_mean_agg,
        "GRADO_OCUPA_POR_HABITACIONES": custom_mean_agg,
        "PERSONAL_EMPLEADO": custom_agg
    }).reset_index()
    
    fields_to_cast = ["VIAJEROS", "PERNOCTACIONES", "ESTABLECIMIENTOS_ESTIMADOS", "HABITACIONES_ESTIMADAS", "PLAZAS_ESTIMADAS", "PERSONAL_EMPLEADO"]
    df[fields_to_cast] = df[fields_to_cast].astype('Int64')
        
    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]
                
    return df


def eoh_pais(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum"
    }).reset_index()
        
    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]
    return df

def eoh_punt_tur(df, fields):
    def custom_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.sum()

    def custom_mean_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.mean()

    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": custom_agg,
        "PERNOCTACIONES": custom_agg,
        "ESTANCIA_MEDIA": custom_mean_agg,
        "ESTABLECIMIENTOS_ESTIMADOS": custom_agg,
        "HABITACIONES_ESTIMADAS": custom_agg,
        "PLAZAS_ESTIMADAS": custom_agg,
        "GRADO_OCUPA_PLAZAS": custom_mean_agg,
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": custom_mean_agg,
        "GRADO_OCUPA_POR_HABITACIONES": custom_mean_agg,
        "PERSONAL_EMPLEADO": custom_agg
    }).reset_index()
    
    fields_to_cast = ["VIAJEROS", "PERNOCTACIONES", "ESTABLECIMIENTOS_ESTIMADOS", "HABITACIONES_ESTIMADAS", "PLAZAS_ESTIMADAS", "PERSONAL_EMPLEADO"]
    df[fields_to_cast] = df[fields_to_cast].astype('Int64')
    
    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]
    return df

def eoh_zona_tur(df, fields):
    def custom_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.sum()

    def custom_mean_agg(x):
        if x.isnull().all():
            return np.nan
        else:
            return x.mean()

    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": custom_agg,
        "PERNOCTACIONES": custom_agg,
        "ESTANCIA_MEDIA": custom_mean_agg,
        "ESTABLECIMIENTOS_ESTIMADOS": custom_agg,
        "HABITACIONES_ESTIMADAS": custom_agg,
        "PLAZAS_ESTIMADAS": custom_agg,
        "GRADO_OCUPA_PLAZAS": custom_mean_agg,
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": custom_mean_agg,
        "GRADO_OCUPA_POR_HABITACIONES": custom_mean_agg,
        "PERSONAL_EMPLEADO": custom_agg
    }).reset_index()
    
    fields_to_cast = ["VIAJEROS", "PERNOCTACIONES", "ESTABLECIMIENTOS_ESTIMADOS", "HABITACIONES_ESTIMADAS", "PLAZAS_ESTIMADAS", "PERSONAL_EMPLEADO"]
    df[fields_to_cast] = df[fields_to_cast].astype('Int64')
    
    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoac_categoria(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PARCELAS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PARCELAS": "mean",
        "GRADO_OCUPA_PARCELAS_FIN_SEMANA": "mean",
        "NUM_PARCELAS_OCUPADAS": "sum",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def eoac_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PARCELAS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PARCELAS": "mean",
        "GRADO_OCUPA_PARCELAS_FIN_SEMANA": "mean",
        "NUM_PARCELAS_OCUPADAS": "sum",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def eoac_pais(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum"
    }).reset_index()

    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoac_provincia(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PARCELAS": "sum",
        "GRADO_OCUPA_PARCELAS": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def eoac_punt_tur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PARCELAS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PARCELAS": "mean",
        "GRADO_OCUPA_PARCELAS_FIN_SEMANA": "mean",
        "NUM_PARCELAS_OCUPADAS": "sum",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():        
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def eoac_zona_tur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PARCELAS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PARCELAS": "mean",
        "GRADO_OCUPA_PARCELAS_FIN_SEMANA": "mean",
        "NUM_PARCELAS_OCUPADAS": "sum",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoal_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PLAZAS": "mean",
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoal_pais(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoap_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "APARTAMENTOS_ESTIMADOS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PLAZAS": "mean",
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": "mean",
        "GRADO_OCUPA_APART": "mean",
        "GRADO_OCUPA_APART_FIN_SEMANA": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoap_pais(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoap_provincia(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "APARTAMENTOS_ESTIMADOS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PLAZAS": "mean",
        "GRADO_OCUPA_APART": "mean",
        "GRADO_OCUPA_APART_FIN_SEMANA": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoap_punt_tur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "APARTAMENTOS_ESTIMADOS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PLAZAS": "mean",
        "GRADO_OCUPA_APART": "mean",
        "GRADO_OCUPA_APART_FIN_SEMANA": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eoap_zona_tur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "APARTAMENTOS_ESTIMADOS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PLAZAS": "mean",
        "GRADO_OCUPA_APART": "mean",
        "GRADO_OCUPA_APART_FIN_SEMANA": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eotr_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PLAZAS": "mean",
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": "mean",
        "GRADO_OCUPA_HABITACIONES": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eotr_pais(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eotr_provincia(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PLAZAS": "mean",
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": "mean",
        "GRADO_OCUPA_HABITACIONES": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def eotr_zona_tur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum",
        "PERNOCTACIONES": "sum",
        # "ESTANCIA_MEDIA": "sum",
        "ESTABLECIMIENTOS_ESTIMADOS": "sum",
        "PLAZAS_ESTIMADAS": "sum",
        "GRADO_OCUPA_PLAZAS": "mean",
        "GRADO_OCUPA_PLAZAS_FIN_SEMANA": "mean",
        "GRADO_OCUPA_HABITACIONES": "mean",
        "PERSONAL_EMPLEADO": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def precios_eoac_categoria(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_CATEGORIA_EOAC": "sum",
        "TASA_VARIACION_CATEGORIA_EOAC": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def precios_eoac_tarifa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_TARIFA_EOAC": "sum",
        "TASA_VARIACION_TARIFA_EOAC": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def precios_eoap_modalidad(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_MODALIDAD_EOAP": "sum",
        "TASA_VARIACION_MODALIDAD_EOAP": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def precios_eoap_tarifa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_TARIFA_EOAP": "sum",
        "TASA_VARIACION_TARIFA_EOAP": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def precios_eoh_categoria(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_CATEGORIA_EOH": "sum",
        "TASA_VARIACION_CATEGORIA_EOH": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def precios_eoh_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_CCAA_EOH": "sum",
        "TASA_VARIACION_CCAA_EOH": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def precios_eotr_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_CCAA_EOTR": "sum",
        "TASA_VARIACION_CCAA_EOTR": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def precios_eotr_modalidad(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_MODALIDAD_EOTR": "sum",
        "TASA_VARIACION_MODALIDAD_EOTR": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df.replace("Total aquiler", "Total alquiler")

def precios_eotr_tarifa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_TARIFA_EOTR": "sum",
        "TASA_VARIACION_TARIFA_EOTR": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def puertos(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "PASAJEROS_TOTALES": "sum",
        "PASAJEROS_CRUCERO": "sum",
        "CRUCEROS": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ind_rentabilidad_categoria(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICADOR_ADR": "sum",
        "TASA_VARIACION_ADR": "sum",
        "INDICADOR_RVPAR": "sum",
        "TASA_VARIACION_RVPAR": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ind_rentabilidad_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICADOR_ADR": "sum",
        "TASA_VARIACION_ADR": "sum",
        "INDICADOR_RVPAR": "sum",
        "TASA_VARIACION_RVPAR": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ind_rentabilidad_provincia(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICADOR_ADR": "sum",
        "TASA_VARIACION_ADR": "sum",
        "INDICADOR_RVPAR": "sum",
        "TASA_VARIACION_RVPAR": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ind_rentabilidad_punt_tur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICADOR_ADR": "sum",
        "TASA_VARIACION_ADR": "sum",
        "INDICADOR_RVPAR": "sum",
        "TASA_VARIACION_RVPAR": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ind_rentabilidad_zona_tur(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICADOR_ADR": "sum",
        "TASA_VARIACION_ADR": "sum",
        "INDICADOR_RVPAR": "sum",
        "TASA_VARIACION_RVPAR": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ipc_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_CCAA_RUBRICAS": "sum",
        "TASA_VARIACION_MENSUAL_CCAA": "sum",
        "TASA_VARIACION_ANUAL_CCAA": "sum",
        "TASA_VARIACION_ACUMULADO_CCAA": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ipc_ccaa_media_anual(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "MEDIA_ANUAL_CCAA_RUBRICAS": "sum",
        "TASA_VARIACION_MEDIAS_ANUALES_CCAA": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ipc_nacional(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_NACIONAL_RUBRICAS": "sum",
        "TASA_VARIACION_MENSUAL_NACIONAL": "sum",
        "TASA_VARIACION_ANUAL_NACIONAL": "sum",
        "TASA_VARIACION_ACUMULADO_NACIONAL": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ipc_nacional_media_anual(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "MEDIA_ANUAL_NACIONAL_RUBRICAS": "sum",
        "TASA_VARIACION_MEDIAS_ANUALES_NACIONAL": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ipc_ponderacion_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "PONDERACION_CCAA_RUBRICAS": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def ipc_ponderacion_nacional(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "PONDERACION_NACIONAL_RUBRICAS": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def iass_negocio(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_GENERAL_NEGOCIO": "sum",
        "TASA_VARIACION_MENSUAL_NEGOCIO": "sum",
        "TASA_VARIACION_ANUAL_NEGOCIO": "sum",
        "TASA_VARIACION_ACUMULADO_NEGOCIO": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def iass_ocupacion(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "INDICE_GENERAL_OCUPACION": "sum",
        "TASA_VARIACION_MENSUAL_OCUPACION": "sum",
        "TASA_VARIACION_ANUAL_OCUPACION": "sum",
        "TASA_VARIACION_ACUMULADO_OCUPACION": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cst_aportacion_empleo_total(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "NUM_APORTACION_EMPLEO_TOTAL": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cst_aportacion_turismo_pib(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "NUM_VALOR_ABSOLUTO": "sum",
        "NUM_PORCENTAJE_PIB": "sum",
        "NUM_INDICE_VOLUMEN": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cst_empleo_turistico(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "NUM_EMPLEO_TURISTICO": "sum",
        "NUM_APORTACION_EMPLEO_TOTAL": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cst_empresa_industria_turisticas(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "NUM_EMPRESAS": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cst_gasto_consumo_turistico(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "NUM_GASTO_INTERIOR": "sum",
        "NUM_GASTO_INTERNO": "sum",
        "NUM_GASTO_EMISOR": "sum",
        "NUM_CONSUMO_TURISTICO_INTERIOR": "sum",
        "NUM_CONSUMO_OTROS_COMPONENTES_TURISTICOS": "sum",
        "NUM_GASTO_RECEPTOR": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cst_gasto_turistico(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "NUM_GASTO_RECEPTOR": "sum",
        "NUM_GASTO_INTERNO": "sum",
        "NUM_GASTO_EMISOR": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cst_locales_industria_turisticas(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "NUM_LOCALES": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def bp_cuenta_bienes_servicios(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "NUM_TOTAL_INGRESOS": "sum",
        "NUM_TOTAL_PAGOS": "sum",
        "NUM_SALDO": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def bp_ingresos_zonas(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "TOTAL_INGRESOS": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def get_balance(balance):
    return balance["Servicios"] + (-1 * balance["Bienes"] if balance["Bienes"] >= 0 else balance["Bienes"])

def bp_saldo(df, fields):
    if fields.keys():
        df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
            "TOTAL_SALDO": "sum",
        }).reset_index()
    else:
        df = df.groupby(["AÑO", "MES"])["DESC_SALDO", "TOTAL_SALDO"].agg(lambda x: x.tolist()).reset_index()
        df['TIPO_SALDO'] = df.apply(lambda x: dict(zip(x['DESC_SALDO'], x['TOTAL_SALDO'])), axis=1)
        df["TOTAL_SALDO"] = df.apply(
            lambda x: get_balance(x.TIPO_SALDO), axis=1)
        
        df = df.drop(['TIPO_SALDO', 'DESC_SALDO'], axis = 1)
        logging.info(df)
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cobertura_tecnologia(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
		"FTTH": "sum",
		"HFC": "sum",
		"4G": "sum",
        "5G": "sum",
        "Inalámbrico fijo": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def cobertura_velocidad(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
		"Cob. 30Mbps": "sum",
		"Cob. 100Mbps": "sum",
        "Cob. 1Gbps": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            df = df.loc[(df[field] == fields[field])]

    return df

def sepe_contrato(df, fields):
    df = df.groupby(["AÑO", "MES", "MUNICIPIO"] + list(fields.keys())).agg({
        "NUM_TOT_CONTRATO": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def sepe_ocupacion(df, fields):
    df = df.groupby(["AÑO", "MES", "MUNICIPIO"] + list(fields.keys())).agg({
        "NUM_PARADOS": "sum",
        "NUM_NO_PARADOS": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def sepe_paro_registrado(df, fields):
    df = df.groupby(["AÑO", "MES", "MUNICIPIO"] + list(fields.keys())).agg({
        "NUM_PARO_REGISTRADO": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def sepe_tipo_demandante(df, fields):
    df = df.groupby(["AÑO", "MES", "MUNICIPIO"] + list(fields.keys())).agg({
        "NUM_TOT_DEMANDANTE": "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def transporte_terrestre(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
        "VIAJEROS": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def afiliacion_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES", "OCUPACION"] + list(fields.keys())).agg({
        "CIFRAS_ABSOLUTAS": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]
                
    return df

def afiliacion_economia_servicios_turismo(df, fields):
    df = df.groupby(["AÑO", "MES", "TIPO_SERVICIOS"] + list(fields.keys())).agg({
        "CIFRAS_ABSOLUTAS": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def afiliacion_turismo(df, fields):
    df = df.groupby(["AÑO", "MES", "TIPO_SERVICIOS"] + list(fields.keys())).agg({
        "CIFRAS_ABSOLUTAS": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def epa_economia_servicios_turismo(df, fields):
    df = df.groupby(["AÑO", "MES", "TIPO_SERVICIOS"] + list(fields.keys())).agg({
        "CIFRAS_ABSOLUTAS": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def epa_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES", "OCUPACION"] + list(fields.keys())).agg({
        "CIFRAS_ABSOLUTAS": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def epa_turismo(df, fields):
    df = df.groupby(["AÑO", "MES", "TIPO_SERVICIOS"] + list(fields.keys())).agg({
        "CIFRAS_ABSOLUTAS": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]
                
    return df

def actividades_ocio(df, fields):
    df = df.groupby(["AÑO", "MES", "PRODUCTO", "CATEGORIA"] + list(fields.keys())).agg({
        "ENTRADAS": "sum",
        "VISITAS_PAGINAS": "sum",
        "GASTO_TOTAL": "sum",
        "PRECIO_MEDIO_ENTRADA": "mean",
        "TRANSACCIONES": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def calidad_aire(df, fields):
    df = df.groupby(["AÑO", "MES","ESTACION","CALIDAD_AIRE"] + list(fields.keys())).agg({
        "PORCENTAJE_CALIDAD_AIRE": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df
  
def policia_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES", "DELITO"] + list(fields.keys())).agg({
        "TASA_DELITOS": "sum",
        "TASA_DELITOS_ESPAÑA": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def policia_provincia(df, fields):
    df = df.groupby(["AÑO", "MES", "DELITO"] + list(fields.keys())).agg({
        "TASA_DELITOS": "sum",
        "TASA_DELITOS_ESPAÑA": "sum"
    }).reset_index()
    
    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def aemet_precipitacion(df, fields):
    df = df.groupby(["AÑO", "MES", "ESTACION_DE_MEDICION"] + list(fields.keys())).agg({
        "PRECIPITACION_ACUMULADA": "sum",
        "INTENSIDAD_MAXIMA_PRECIPITACION": "max"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def aemet_temperatura(df, fields):
    df = df.groupby(["AÑO", "MES", "ESTACION_DE_MEDICION"] + list(fields.keys())).agg({
        "TEMPERATURA_MINIMA": "min",
        "TEMPERATURA_MAXIMA": "max",
        "TEMPERATURA_MEDIA": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def aemet_viento(df, fields):
    df = df.groupby(["AÑO", "MES", "ESTACION_DE_MEDICION"] + list(fields.keys())).agg({
        "VELOCIDAD_MEDIA_DE_RACHAS_MAXIMAS": "max"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def aemet_sol(df, fields):
    df = df.groupby(["AÑO", "MES", "ESTACION_DE_MEDICION"] + list(fields.keys())).agg({
        "HORAS_DE_SOL_DIARIAS": "mean",
        "INDICE_UV": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def global_blue(df, fields):
    df = df.groupby(["AÑO", "MES","NACIONALIDAD","CLASE_COMPRADOR","RANGO_EDAD"] + list(fields.keys())).agg({
        "GASTO_MEDIO": "mean",
        "PORCENTAJE_TRANSACCIONES": "mean",
        "PORCENTAJE_COMPRADORES": "mean",
        "PORCENTAJE_GASTO": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def mabrian_indices(df, fields):
    df = df.groupby(["AÑO", "MES","PAIS_ORIGEN"] + list(fields.keys())).agg({
        "INIDICE_PERCEPCION_TURISTICA_GLOBAL": "mean",
        "INDICE_SATISFACCION_PRODUCTOS_TURISTICOS": "mean",
        "INDICE_PERCEPCION_SEGURIDAD": "mean",
        "INDICE_PERCEPCION_CLIMATICA": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def mabrian_alojamientos(df, fields):
    logging.info(df.columns)
    df = df.groupby(["AÑO", "MES","PAIS_ORIGEN", "CATEGORIA_ALOJAMIENTO"] + list(fields.keys())).agg({
        "INDICE_SATISFACCION_HOTELERA": "mean",
        "MENCIONES_RRSS_(%)": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def mabrian_precios(df, fields):
    df = df.groupby(["AÑO", "MES","PERIODO_ANTELACION", "CATEGORIA_ALOJAMIENTO"] + list(fields.keys())).agg({
        "PRECIO_CHECK-IN_ENTRE_SEMANA": "mean",
        "PRECIO_CHECK-IN_FIN_SEMANA": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def proteccion_naturaleza_areas(df, fields):
    df = df.groupby(["AÑO", "MES","AREA_PROTEGIDA"] + list(fields.keys())).agg({
        "SUPERFICIE(ha)": "mean",
        "ESPACIOS": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def proteccion_naturaleza_superficie(df, fields):
    df = df.groupby(["AÑO", "MES","TIPO_SUPERFICIE"] + list(fields.keys())).agg({
        "SUPERFICIE(ha)": "mean",

    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def dirce_empresas_general(df, fields):
    df = df.groupby(["AÑO", "MES", "SERVICIO"] + list(fields.keys())).agg({
        "NUMERO_EMPRESAS": "sum",

    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def dirce_empresas_turismo(df, fields):
    df = df.groupby(["AÑO", "MES", "SERVICIO", "ESTRATO_ASALARIADOS"] + list(fields.keys())).agg({
        "NUMERO_EMPRESAS": "sum",

    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def sictur_tipo_publicaciones(df, fields):
    df = df.groupby(["AÑO", "MES", "PAIS", "TIPO_PUBLICACION"] + list(fields.keys())).agg({
        "PUBLICACIONES": "sum",

    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def sictur_investigaciones(df, fields):
    df = df.groupby([] + list(fields.keys())).agg({
        "INVESTIGADORES": "sum",
        "GRUPOS_INVESTIGACION": "sum"

    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def informa_alojamientos_restauracion(df, fields):
    df = df.groupby(["TIPO_ESTABLECIMIENTO"] + list(fields.keys())).agg({
        "NUM_ESTABLECIMIENTOS": "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def informa_atracciones_recursos_turisticos(df, fields):
    df = df.groupby(["CATEGORIA"] + list(fields.keys())).agg({
        "VALORACION_POR_CATEGORIA": "mean",
        "TOTAL_OPINIONES_CATEGORIA": "sum",
        "VALORACION_GENERAL": "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def calidad_agua(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
      "ZONA_BAÑO_MARÍTIMA" : "sum",
      "ZONA_BAÑO_CONTINENTAL" : "sum",
      "PM_CALIDAD_AGUA_INSUFICIENTE" : "mean",
      "PM_CALIDAD_AGUA_SUFICIENTE" : "mean",
      "PM_CALIDAD_AGUA_BUENA" : "mean",
      "PM_CALIDAD_AGUA_EXCELENTE" : "mean",
      "PM_CALIDAD_AGUA_SIN_CALIFICAR" : "mean",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def turismo_emisor_ccaa_pais(df, fields):
    df = df.groupby(["AÑO", "MES", "CONTINENTE_DESTINO", "PAIS_DESTINO"] + list(fields.keys())).agg({
      "TURISTAS" : "sum",
      "PERNOCTACIONES" : "sum",
      "ESTANCIA_MEDIA" : "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def turismo_emisor_mun_pais(df, fields):
    df = df.groupby(["AÑO", "MES", "MUNICIPIO_ORIGEN", "CONTINENTE_DESTINO", "PAIS_DESTINO"] + list(fields.keys())).agg({
      "TURISTAS" : "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def turismo_emisor_provincia_pais(df, fields):
    df = df.groupby(["AÑO", "MES", "CONTINENTE_DESTINO", "PAIS_DESTINO"] + list(fields.keys())).agg({
      "TURISTAS" : "sum",
      "PERNOCTACIONES" : "sum",
      "ESTANCIA_MEDIA" : "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def turismo_receptor_ccaa_pais(df, fields):
    df = df.groupby(["AÑO", "MES", "CONTINENTE_ORIGEN", "PAIS_ORIGEN"] + list(fields.keys())).agg({
      "TURISTAS" : "sum",
      "PERNOCTACIONES" : "sum",
      "ESTANCIA_MEDIA" : "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def turismo_receptor_mun_pais(df, fields):
    df = df.groupby(["AÑO", "MES", "MUNICIPIO_DESTINO", "CONTINENTE_ORIGEN", "PAIS_ORIGEN"] + list(fields.keys())).agg({
      "TURISTAS" : "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def turismo_receptor_provincia_pais(df, fields):
    df = df.groupby(["AÑO", "MES", "CONTINENTE_ORIGEN", "PAIS_ORIGEN"] + list(fields.keys())).agg({
      "TURISTAS" : "sum",
      "PERNOCTACIONES" : "sum",
      "ESTANCIA_MEDIA" : "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df


def turismo_interno_prov_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
      "TURISTAS" : "sum",
      "PERNOCTACIONES" : "sum",
      "ESTANCIA_MEDIA" : "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def turismo_interior_mun_mun(df, fields):
    df = df.groupby(["AÑO", "MES", "MUNICIPIO_ORIGEN", "MUNICIPIO_DESTINO"] + list(fields.keys())).agg({
      "TURISTAS" : "sum",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def imd_anual(df, fields):
    df = df.groupby(["AÑO", "MES", "CARRETERA", "TIPO_ESTACION", "ESTACION"] + list(fields.keys())).agg({
      "IMD_VEHICULO_LIGERO" : "sum",
      "IMD_VEHICULO_PESADO" : "sum",
      "IMD_VEHICULO_TOTAL" : "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def imd_mensual(df, fields):
    df = df.groupby(["AÑO", "MES", "CARRETERA", "TIPO_ESTACION", "ESTACION"] + list(fields.keys())).agg({
      "IMD_VEHICULO_LIGERO" : "sum",
      "IMD_VEHICULO_PESADO" : "sum",
      "IMD_VEHICULO_TOTAL" : "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def imd_semana(df, fields):
    df = df.groupby(["AÑO", "MES", "DIA_SEMANA", "CARRETERA", "TIPO_ESTACION", "ESTACION"] + list(fields.keys())).agg({
      "IMD_VEHICULO_LIGERO" : "sum",
      "IMD_VEHICULO_PESADO" : "sum",
      "IMD_VEHICULO_TOTAL" : "sum"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def vut_ccaa(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
      "VIVIENDAS_TURISTICAS" : "sum",
      "PLAZAS" : "sum",
      "PLAZAS_POR_VIVIENDA_TURISTICA" : "mean",
      "PORCENTAJE_VIVIENDAS_TURISTICAS" : "mean",
      "PRECIO" : "mean",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def vut_prov(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())).agg({
      "VIVIENDAS_TURISTICAS" : "sum",
      "PLAZAS" : "sum",
      "PLAZAS_POR_VIVIENDA_TURISTICA" : "mean",
      "PORCENTAJE_VIVIENDAS_TURISTICAS" : "mean",
      "PRECIO" : "mean",
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def vut_mun(df, fields):
    df = df.groupby(["AÑO", "MES", "MUNICIPIO"] + list(fields.keys())).agg({
      "VIVIENDAS_TURISTICAS" : "sum",
      "PLAZAS" : "sum",
      "PLAZAS_POR_VIVIENDA_TURISTICA" : "mean",
      "PORCENTAJE_VIVIENDAS_TURISTICAS" : "mean"
    }).reset_index()

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df

def busqueda(df, fields):
    df = df.groupby(["AÑO", "MES"] + list(fields.keys())
                    ).size().reset_index(name='MENCIONES')

    for field in fields.keys():
        if isinstance(fields[field], list):
            df = df.loc[df[field].isin(fields[field])]
        else:
            if fields[field] != "Todos":
                df = df.loc[(df[field] == fields[field])]

    return df