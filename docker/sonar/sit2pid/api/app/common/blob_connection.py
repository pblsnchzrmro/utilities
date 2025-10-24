from azure.storage.blob import BlobClient, BlobServiceClient, PublicAccess
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
import pandas as pd
import numpy as np

from io import BytesIO, StringIO
import time
import logging

from common import groupings

CONTAINER_NAME = "dataestur-api"
account_name = "sitdatalake"
SAS_token = """sp=r&st=2023-06-19T08:42:29Z&se=2027-06-01T16:42:29Z&spr=https&sv=2022-11-02&sr=c&sig=WB%2Fi4qKOL0tQ2WE2ky7QxNefJIRpg%2BevoYkReJR0YKo%3D"""

def read_blob_file(filename, fields, start_year=None, start_month=None, end_year=None, end_month=None, file_type='.csv'):
    blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=SAS_token
)
    # blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blob_client = container_client.get_blob_client(filename)
    try:
        logging.info("Reading file from blob")
        stream = blob_client.download_blob()
        chunk_list = []  # A list to hold our chunks of data

        # Read the blob data in chunks
        for chunk in stream.chunks():
            chunk_list.append(chunk)
        
        # Combine all chunks into a single BytesIO object
        data = BytesIO(b''.join(chunk_list))
        
        df = None
        if file_type == '.xlsx':
            df = pd.read_excel(data)
        elif file_type == '.csv':
            df = pd.read_csv(data, encoding="utf-8", sep=";")

        logging.info("Total rows before date filter {}...".format(len(df.index)))

        df["AÑO"] = df["ANYOMES"].apply(lambda x: int(str(x)[:4]))
        df["MES"] = df["ANYOMES"].apply(lambda x: int(str(x)[-2:]))

        if end_year and end_month:
            start_date = int(str(start_year) + str(start_month).zfill(2))
            end_date = int(str(end_year) + str(end_month).zfill(2))
            df = df.loc[(start_date <= df["ANYOMES"]) & (df["ANYOMES"] <= end_date)]
        else:
            if start_year and start_month:
                start_date = int(str(start_year) + str(start_month).zfill(2))
                df = df.loc[(df["ANYOMES"] >= start_date)]
            elif start_year and not start_month:
                df = df.loc[(df["AÑO"] >= int(start_year))]

        logging.info("Total rows before group by {}...".format(len(df.index)))

        if filename == "/FRONTUR/frontur.csv":
            df = groupings.frontur(df, fields)

        elif filename == "/ETR/etr.csv":
            df = groupings.etr(df, fields)

        elif filename == "/EGATUR/egatur.csv":
            df = groupings.egatur(df, fields)

        elif filename == "/AENA/aena_aerolineas.csv":
            df = groupings.aena_aerolineas(df, fields)
        elif filename == "/AENA/aena_destinos.csv":
            df = groupings.aena_destinos(df, fields)

        elif filename == "/EOH/eoh_categoria.csv":
            df = groupings.eoh_categoria(df, fields)
        elif filename == "/EOH/eoh_prov.csv":
            df = groupings.eoh_prov(df, fields)
        elif filename == "/EOH/eoh_pais.csv":
            df = groupings.eoh_pais(df, fields)
        elif filename == "/EOH/eoh_punt_tur.csv":
            df = groupings.eoh_punt_tur(df, fields)
        elif filename == "/EOH/eoh_zona_tur.csv":
            df = groupings.eoh_zona_tur(df, fields)
        elif filename == "/EOH/eoh_ccaa.csv":
            df = groupings.eoh_ccaa(df, fields)

        elif filename == "/EOAC/eoac_categoria.csv":
            df = groupings.eoac_categoria(df, fields)
        elif filename == "/EOAC/eoac_ccaa.csv":
            df = groupings.eoac_ccaa(df, fields)
        elif filename == "/EOAC/eoac_pais.csv":
            df = groupings.eoac_pais(df, fields)
        elif filename == "/EOAC/eoac_provincia.csv":
            df = groupings.eoac_provincia(df, fields)
        elif filename == "/EOAC/eoac_punt_tur.csv":
            df = groupings.eoac_punt_tur(df, fields)
        elif filename == "/EOAC/eoac_zona_tur.csv":
            df = groupings.eoac_zona_tur(df, fields)

        elif filename == "/EOAL/eoal_ccaa.csv":
            df = groupings.eoal_ccaa(df, fields)
        elif filename == "/EOAL/eoal_pais.csv":
            df = groupings.eoal_pais(df, fields)

        elif filename == "/EOAP/eoap_ccaa.csv":
            df = groupings.eoap_ccaa(df, fields)
        elif filename == "/EOAP/eoap_pais.csv":
            df = groupings.eoap_pais(df, fields)
        elif filename == "/EOAP/eoap_provincia.csv":
            df = groupings.eoap_provincia(df, fields)
        elif filename == "/EOAP/eoap_punt_tur.csv":
            df = groupings.eoap_punt_tur(df, fields)
        elif filename == "/EOAP/eoap_zona_tur.csv":
            df = groupings.eoap_zona_tur(df, fields)

        elif filename == "/EOTR/eotr_ccaa.csv":
            df = groupings.eotr_ccaa(df, fields)
        elif filename == "/EOTR/eotr_pais.csv":
            df = groupings.eotr_pais(df, fields)
        elif filename == "/EOTR/eotr_provincia.csv":
            df = groupings.eotr_provincia(df, fields)
        elif filename == "/EOTR/eotr_zona_tur.csv":
            df = groupings.eotr_zona_tur(df, fields)

        elif filename == "/PRECIOS/eoac_categoria.csv":
            df = groupings.precios_eoac_categoria(df, fields)
        elif filename == "/PRECIOS/eoac_tarifa.csv":
            df = groupings.precios_eoac_tarifa(df, fields)
        elif filename == "/PRECIOS/eoap_modalidad.csv":
            df = groupings.precios_eoap_modalidad(df, fields)
        elif filename == "/PRECIOS/eoap_tarifa.csv":
            df = groupings.precios_eoap_tarifa(df, fields)
        elif filename == "/PRECIOS/eoh_categoria.csv":
            df = groupings.precios_eoh_categoria(df, fields)
        elif filename == "/PRECIOS/eoh_ccaa.csv":
            df = groupings.precios_eoh_ccaa(df, fields)
        elif filename == "/PRECIOS/eotr_ccaa.csv":
            df = groupings.precios_eotr_ccaa(df, fields)
        elif filename == "/PRECIOS/eotr_modalidad.csv":
            df = groupings.precios_eotr_modalidad(df, fields)
        elif filename == "/PRECIOS/eotr_tarifa.csv":
            df = groupings.precios_eotr_tarifa(df, fields)

        elif filename == "/PUERTOS/puertos.csv":
            df = groupings.puertos(df, fields)

        elif filename == "/IND_RENTABILIDAD/ind_rentabilidad_categoria.csv":
            df = groupings.ind_rentabilidad_categoria(df, fields)
        elif filename == "/IND_RENTABILIDAD/ind_rentabilidad_ccaa.csv":
            df = groupings.ind_rentabilidad_ccaa(df, fields)
        elif filename == "/IND_RENTABILIDAD/ind_rentabilidad_provincia.csv":
            df = groupings.ind_rentabilidad_provincia(df, fields)
        elif filename == "/IND_RENTABILIDAD/ind_rentabilidad_punt_tur.csv":
            df = groupings.ind_rentabilidad_punt_tur(df, fields)
        elif filename == "/IND_RENTABILIDAD/ind_rentabilidad_zona_tur.csv":
            df = groupings.ind_rentabilidad_zona_tur(df, fields)

        elif filename == "/IPC/ipc_ccaa.csv":
            df = groupings.ipc_ccaa(df, fields)
        elif filename == "/IPC/ipc_ccaa_media_anual.csv":
            df = groupings.ipc_ccaa_media_anual(df, fields)
        elif filename == "/IPC/ipc_nacional.csv":
            df = groupings.ipc_nacional(df, fields)
        elif filename == "/IPC/ipc_nacional_media_anual.csv":
            df = groupings.ipc_nacional_media_anual(df, fields)
        elif filename == "/IPC/ipc_ponderacion_ccaa.csv":
            df = groupings.ipc_ponderacion_ccaa(df, fields)
        elif filename == "/IPC/ipc_ponderacion_nacional.csv":
            df = groupings.ipc_ponderacion_nacional(df, fields)

        elif filename == "/IASS/iass_negocio.csv":
            df = groupings.iass_negocio(df, fields)
        elif filename == "/IASS/iass_ocupacion.csv":
            df = groupings.iass_ocupacion(df, fields)

        elif filename == "/CST/cst_aportacion_empleo_total.csv":
            df = groupings.cst_aportacion_empleo_total(df, fields)
        elif filename == "/CST/cst_aportacion_turismo_pib.csv":
            df = groupings.cst_aportacion_turismo_pib(df, fields)
        elif filename == "/CST/cst_empleo_turistico.csv":
            df = groupings.cst_empleo_turistico(df, fields)
        elif filename == "/CST/cst_empresa_industria_turisticas.csv":
            df = groupings.cst_empresa_industria_turisticas(df, fields)
        elif filename == "/CST/cst_gasto_consumo_turistico.csv":
            df = groupings.cst_gasto_consumo_turistico(df, fields)
        elif filename == "/CST/cst_gasto_turistico.csv":
            df = groupings.cst_gasto_turistico(df, fields)
        elif filename == "/CST/cst_locales_industria_turisticas.csv":
            df = groupings.cst_locales_industria_turisticas(df, fields)

        elif filename == "/BP/bp_cuenta_bienes_servicios.csv":
            df = groupings.bp_cuenta_bienes_servicios(df, fields)
        elif filename == "/BP/bp_ingresos_zonas.csv":
            df = groupings.bp_ingresos_zonas(df, fields)
        elif filename == "/BP/bp_saldo.csv":
            df = groupings.bp_saldo(df, fields)

        elif filename == "/COBERTURA/cobertura_ccaa_tecnologia.csv":
            df = groupings.cobertura_tecnologia(df, fields)
        elif filename == "/COBERTURA/cobertura_ccaa_velocidad.csv":
            df = groupings.cobertura_velocidad(df, fields)
        elif filename == "/COBERTURA/cobertura_provincia_tecnologia.csv":
            df = groupings.cobertura_tecnologia(df, fields)
        elif filename == "/COBERTURA/cobertura_provincia_velocidad.csv":
            df = groupings.cobertura_velocidad(df, fields)

        elif filename == "/SEPE/sepe_contrato.csv":
            df = groupings.sepe_contrato(df, fields)
        elif filename == "/SEPE/sepe_ocupacion.csv":
            df = groupings.sepe_ocupacion(df, fields)
        elif filename == "/SEPE/sepe_paro_registrado.csv":
            df = groupings.sepe_paro_registrado(df, fields)
        elif filename == "/SEPE/sepe_tipo_demandante.csv":
            df = groupings.sepe_tipo_demandante(df, fields)
        
        elif filename == "/TRANSPORTE/transporte_terrestre.csv":
            df = groupings.transporte_terrestre(df, fields)
        
        elif filename == "/AFILIACION/afiliacion_ccaa.csv":
            df = groupings.afiliacion_ccaa(df, fields)    
        elif filename == "/AFILIACION/afiliacion_economia_servicios_turismo.csv":
            df = groupings.afiliacion_economia_servicios_turismo(df, fields)
        elif filename == "/AFILIACION/afiliacion_turismo.csv":
            df = groupings.afiliacion_turismo(df, fields)

        elif filename == "/EPA/epa_economia_servicios_turismo.csv":
            df = groupings.epa_economia_servicios_turismo(df, fields)
        elif filename == "/EPA/epa_ccaa.csv":
            df = groupings.epa_ccaa(df, fields)
        elif filename == "/EPA/epa_turismo.csv":
            df = groupings.epa_turismo(df, fields)
            
        elif filename == "/ACTIVIDADES_OCIO/actividades_ocio.csv":
            df = groupings.actividades_ocio(df, fields)   

        elif filename == "/CALIDAD_AIRE/calidad_aire.csv":
            df = groupings.calidad_aire(df, fields)
            
        elif filename == "/POLICIA/policia_ccaa.csv":
            df = groupings.policia_ccaa(df, fields)
        elif filename == "/POLICIA/policia_provincia.csv":
            df = groupings.policia_provincia(df, fields)
        
        elif filename == "/AEMET/precipitacion.csv":
            df = groupings.aemet_precipitacion(df, fields)
        elif filename == "/AEMET/temperatura.csv":
            df = groupings.aemet_temperatura(df, fields)
        elif filename == "/AEMET/viento.csv":
            df = groupings.aemet_viento(df, fields)
        elif filename == "/AEMET/sol.csv":
            df = groupings.aemet_sol(df, fields)

        elif filename == "/GLOBAL_BLUE/global_blue.csv":
            df = groupings.global_blue(df, fields)    
                          
        elif filename == "/MABRIAN/mabrian_indices.csv":
            df = groupings.mabrian_indices(df, fields)
        elif filename == "/MABRIAN/mabrian_alojamientos.csv":
            df = groupings.mabrian_alojamientos(df, fields)
        elif filename == "/MABRIAN/mabrian_precios.csv":
            df = groupings.mabrian_precios(df, fields)

        elif filename == "/PROTECCION_NATURALEZA/areas_protegidas_ccaa.csv":
            df = groupings.proteccion_naturaleza_areas(df, fields)
        elif filename == "/PROTECCION_NATURALEZA/superficie_protegida.csv":
            df = groupings.proteccion_naturaleza_superficie(df, fields)
            
        elif filename == "/DIRCE/empresas_general.csv":
            df = groupings.dirce_empresas_general(df, fields)
        elif filename == "/DIRCE/empresas_turismo.csv":
            df = groupings.dirce_empresas_turismo(df, fields)

        elif filename == "/SICTUR/sictur_tipo_publicaciones.csv":
            df = groupings.sictur_tipo_publicaciones(df, fields)
        elif filename == "/DIRCE/empresas_turismo.csv":
            df = groupings.dirce_empresas_turismo(df, fields)
            
        elif filename == "/CALIDAD_AGUA/calidad_agua.csv":
            df = groupings.calidad_agua(df, fields)
        
        elif filename == "/TELEFONIA_TURISMO/turismo_emisor_ccaa_pais.csv":
            df = groupings.turismo_emisor_ccaa_pais(df, fields)
        elif filename == "/TELEFONIA_TURISMO/turismo_emisor_mun_pais.csv":
            df = groupings.turismo_emisor_mun_pais(df, fields)
        elif filename == "/TELEFONIA_TURISMO/turismo_emisor_provincia_pais.csv":
            df = groupings.turismo_emisor_provincia_pais(df, fields)
        elif filename == "/TELEFONIA_TURISMO/turismo_receptor_ccaa_pais.csv":
            df = groupings.turismo_receptor_ccaa_pais(df, fields)
        elif filename == "/TELEFONIA_TURISMO/turismo_receptor_mun_pais.csv":
            df = groupings.turismo_receptor_mun_pais(df, fields)
        elif filename == "/TELEFONIA_TURISMO/turismo_receptor_provincia_pais.csv":
            df = groupings.turismo_receptor_provincia_pais(df, fields)
        elif filename == "/TELEFONIA_TURISMO/turismo_interno_prov_ccaa.csv":
            df = groupings.turismo_interno_prov_ccaa(df, fields)
            
        elif filename == "/INTENSIDAD_TRAFICO/imd_anual.csv":
            df = groupings.imd_anual(df, fields)    
        elif filename == "/INTENSIDAD_TRAFICO/imd_mensual.csv":
            df = groupings.imd_mensual(df, fields)
        elif filename == "/INTENSIDAD_TRAFICO/imd_semana.csv":
            df = groupings.imd_semana(df, fields)
            
        elif filename == "/VIVIENDA_TURISTICA/vut_ccaa.csv":
            df = groupings.vut_ccaa(df, fields)
        elif filename == "/VIVIENDA_TURISTICA/vut_prov.csv":
            df = groupings.vut_prov(df, fields)
        elif filename == "/VIVIENDA_TURISTICA/vut_mun.csv":
            df = groupings.vut_mun(df, fields)

        elif filename == "/ESCUCHA_ACTIVA/busqueda.csv":
            df = groupings.busqueda(df, fields)

        return df
    
    except ResourceNotFoundError as e:
        logging.warning("Requested file is not in found...")
        return None


def read_blob_file_without_anyomes(filename, fields, file_type='.csv'):
    blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=SAS_token
)

    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blob_client = container_client.get_blob_client(filename)
    try:
        logging.info("Reading file from blob")
        data = BytesIO(blob_client.download_blob().readall())
        df = None
        if file_type == '.xlsx':
            df = pd.read_excel(data)
        elif file_type == '.csv':
            df = pd.read_csv(data, encoding="utf-8", sep=";")

        logging.info("Total rows before group by {}...".format(len(df.index)))

        if filename == "/SICTUR/sictur_investigaciones.csv":
            df = groupings.sictur_investigaciones(df, fields)
            
        elif filename == "/INFORMA/alojamientos_restauracion.csv":
            df = groupings.informa_alojamientos_restauracion(df, fields)
        elif filename == "/INFORMA/atracciones_recursos_turisticos.csv":
            df = groupings.informa_atracciones_recursos_turisticos(df, fields)
            
        elif "/TELEFONIA_TURISMO/turismo_interno_mun_mun" in filename:
            df["AÑO"] = df["ANYOMES"].apply(lambda x: int(str(x)[:4]))
            df["MES"] = df["ANYOMES"].apply(lambda x: int(str(x)[-2:]))
            df = groupings.turismo_interior_mun_mun(df, fields)
            
        return df
    
    except ResourceNotFoundError as e:
        logging.warning("Requested file is not in found...")
        return None


def get_df_bytes_xlsx(df):
    to_write = BytesIO()
    df.to_excel(to_write, index=False)  # write to BytesIO buffer
    to_write.seek(0)
    return to_write.getvalue()


def get_df_bytes_csv(df):
    to_write = StringIO()
    #Replace "." with "," in numeric columns for open corretly in excel
    for columna in df.select_dtypes(include=[float, int]).columns:
            df[columna] = df[columna].astype(str).str.replace('.', ',', regex = False).replace('nan', np.nan)
            
    df.to_csv(to_write, header=True, encoding='ISO-8859-1', index=False, sep = ";")  # write to StringIO buffer
    to_write.seek(0)
    return to_write.getvalue().encode('ISO-8859-1')