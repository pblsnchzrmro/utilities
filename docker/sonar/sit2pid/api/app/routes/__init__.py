from flask import Flask

from .actividades_ocio import actividades_ocio_bp
from .aena_aerolineas import aena_aerolineas_bp
from .aena_destinos import aena_destinos_bp
from .afiliacion_ccaa import afiliacion_ccaa_bp
from .afiliacion_economia_servicios_turismo import afiliacion_economia_servicios_turismo_bp
from .afiliacion_turismo import afiliacion_turismo_bp
from .alojamientos_restauracion import alojamientos_restauracion_bp
from .atracciones_recursos_turisticos import atracciones_recursos_turisticos_bp
from .balanza_de_pagos_cuenta_bienes_servicios import balanza_de_pagos_cuenta_bienes_servicios_bp
from .balanza_de_pagos_ingresos import balanza_de_pagos_ingresos_bp
from .balanza_de_pagos_saldo import balanza_de_pagos_saldo_bp
from .calidad_agua import calidad_agua_bp
from .calidad_aire import calidad_aire_bp
from .cobertura_tecnologia_ccaa import cobertura_tecnologia_ccaa_bp
from .cobertura_tecnologia_provincia import cobertura_tecnologia_provincia_bp
from .cobertura_velocidad_ccaa import cobertura_velocidad_ccaa_bp
from .cobertura_velocidad_provincia import cobertura_velocidad_provincia_bp
from .cst_aportacion_turismo_pib import cst_aportacion_turismo_pib_bp
from .cst_empleo_turistico import cst_empleo_turistico_bp
from .cst_empresas_industria_turisticas import cst_empresas_industrias_turisticas_bp
from .cst_gasto_consumo_turistico import cst_gasto_consumo_turistico_bp
from .cst_locales_industrias_turisticas import cst_locales_industrias_turisticas_bp
from .dirce_empresas_general import dirce_empresas_general_bp
from .dirce_empresas_turismo import dirce_empresas_turismo_bp
from .diva import diva_bp
from .egatur import egatur_bp
from .eoac_categoria import eoac_categoria_bp
from .eoac_ccaa import eoac_ccaa_bp
from .eoac_pais import eoac_pais_bp
from .eoac_provincia import eoac_provincia_bp
from .eoac_punt_tur import eoac_punt_tur_bp
from .eoac_zona_tur import eoac_zona_tur_bp
from .eoal_ccaa import eoal_ccaa_bp
from .eoal_pais import eoal_pais_bp
from .eoap_ccaa import eoap_ccaa_bp
from .eoap_pais import eoap_pais_bp
from .eoap_provincia import eoap_provincia_bp
from .eoap_punt_tur import eoap_punt_tur_bp
from .eoap_zona_tur import eoap_zona_tur_bp
from .eoh_categoria import eoh_categoria_bp
from .eoh_ccaa import eoh_ccaa_bp
from .eoh_pais import eoh_pais_bp
from .eoh_prov import eoh_prov_bp
from .eoh_punt_tur import eoh_punt_tur_bp
from .eoh_zona_tur import eoh_zona_tur_bp
from .etr import etr_bp
from .frontur import frontur_bp
from .vivienda_turistica_prov import vivienda_turistica_prov_bp
from .vivienda_turistica_mun import vivienda_turistica_mun_bp
from .vivienda_turistica_ccaa import vivienda_turistica_ccaa_bp
from .valores_climatologicos_viento import valores_climatologicos_viento_bp
from .valores_climatologicos_temperatura import valores_climatologicos_temperatura_bp
from .valores_climatologicos_sol import valores_climatologicos_sol_bp
from .valores_climatologicos_precipitacion import valores_climatologicos_precipitacion_bp
from .turismo_receptor_prov_pais import turismo_receptor_prov_pais_bp
from .turismo_receptor_mun_pais import turismo_receptor_mun_pais_bp
from .turismo_receptor_ccaa_pais import turismo_receptor_ccaa_pais_bp
from .turismo_interno_prov_ccaa import turismo_interno_prov_ccaa_bp
from .turismo_interno_mun_mun import turismo_interno_mun_mun_bp
from .turismo_emisor_prov_pais import turismo_emisor_prov_pais_bp
from .turismo_emisor_mun_pais import turismo_emisor_mun_pais_bp
from .turismo_emisor_ccaa_pais import turismo_emisor_ccaa_pais_bp
from .transporte_terrestre import transporte_terrestre_bp
from .sepe_tipo_demandante import sepe_tipo_demandante_bp
from .sepe_paro_registrado import sepe_paro_registrado_bp
from .sepe_ocupacion import sepe_ocupacion_bp
from .sepe_contrato import sepe_contrato_bp
from .seguridad_provincia import seguridad_provincia_bp
from .seguridad_ccaa import seguridad_ccaa_bp
from .puertos import puertos_bp
from .precios_eotr_tarifa import precios_eotr_tarifa_bp
from .precios_eotr_modalidad import precios_eotr_modalidad_bp
from .precios_eotr_ccaa import precios_eotr_ccaa_bp
from .precios_eoh_ccaa import precios_eoh_ccaa_bp
from .precios_eoh_categoria import precios_eoh_categoria_bp
from .precios_eoap_tarifa import precios_eoap_tarifa_bp
from .precios_eoap_modalidad import precios_eoap_modalidad_bp
from .precios_eoac_tarifa import precios_eoac_tarifa_bp
from .precios_eoac_categoria import precios_eoac_categoria_bp
from .precios_alojamientos_hoteleros import precios_alojamientos_hoteleros_bp
from .naturaleza_superficie_protegida import naturaleza_superficie_protegida_bp
from .naturaleza_areas_protegidas_ccaa import naturaleza_areas_protegidas_ccaa_bp
from .museo import museo_bp
from .ipc_ponderacion_ccaa import ipc_ponderacion_ccaa_bp
from .ipc_nacional_media_anual import ipc_nacional_media_anual_bp
from .ipc_nacional import ipc_nacional_bp
from .ipc_ccaa import ipc_ccaa_bp
from .ipc_ccaa_media_anual import ipc_ccaa_media_anual_bp
from .intensidad_trafico_semana import intensidad_trafico_semana_bp
from .intensidad_trafico_mensual import intensidad_trafico_mensual_bp
from .intensidad_trafico_anual import intensidad_trafico_anual_bp
from .ind_satisfaccion_percepcion import ind_satisfaccion_percepcion_bp
from .ind_satisfaccion_menciones_hotel import ind_satisfaccion_menciones_hotel_bp
from .ind_rentabilidad_zona_tur import ind_rentabilidad_zona_tur_bp
from .ind_rentabilidad_punt_tur import ind_rentabilidad_punt_tur_bp
from .ind_rentabilidad_provincia import ind_rentabilidad_provincia_bp
from .ind_rentabilidad_ccaa import ind_rentabilidad_ccaa_bp
from .ind_rentabilidad_categoria import ind_rentabilidad_categoria_bp
from .iass_ocupacion import iass_ocupacion_bp
from .iass_negocio import iass_negocio_bp
from .escucha_activa import escucha_activa_bp
from .epa_turismo import epa_turismo_bp
from .epa_economia_servicios_turismo import epa_economia_servicios_turismo_bp
from .epa_ccaa import epa_ccaa_bp
from .eotr_zona_tur import eotr_zona_tur_bp
from .eotr_provincia import eotr_provincia_bp
from .eotr_pais import eotr_pais_bp
from .eotr_ccaa import eotr_ccaa_bp

from flask_cors import CORS

def create_app():
    app = Flask(__name__)
    CORS(app, resources={r"/*": {"origins": "*"}})
    app.config['CORS_HEADERS'] = 'Content-Type'

    app.register_blueprint(actividades_ocio_bp)

    app.register_blueprint(aena_aerolineas_bp)
    app.register_blueprint(aena_destinos_bp)

    app.register_blueprint(afiliacion_ccaa_bp)
    app.register_blueprint(afiliacion_economia_servicios_turismo_bp)
    app.register_blueprint(afiliacion_turismo_bp)   

    app.register_blueprint(alojamientos_restauracion_bp)

    app.register_blueprint(atracciones_recursos_turisticos_bp)

    app.register_blueprint(balanza_de_pagos_cuenta_bienes_servicios_bp)
    app.register_blueprint(balanza_de_pagos_ingresos_bp)
    app.register_blueprint(balanza_de_pagos_saldo_bp)

    app.register_blueprint(calidad_agua_bp)
    app.register_blueprint(calidad_aire_bp)

    app.register_blueprint(cobertura_tecnologia_ccaa_bp)
    app.register_blueprint(cobertura_tecnologia_provincia_bp)
    app.register_blueprint(cobertura_velocidad_ccaa_bp)
    app.register_blueprint(cobertura_velocidad_provincia_bp)

    app.register_blueprint(cst_aportacion_turismo_pib_bp)
    app.register_blueprint(cst_empleo_turistico_bp)
    app.register_blueprint(cst_empresas_industrias_turisticas_bp)
    app.register_blueprint(cst_gasto_consumo_turistico_bp)
    app.register_blueprint(cst_locales_industrias_turisticas_bp)

    app.register_blueprint(dirce_empresas_general_bp)
    app.register_blueprint(dirce_empresas_turismo_bp)

    app.register_blueprint(diva_bp)

    app.register_blueprint(egatur_bp)

    app.register_blueprint(eoac_categoria_bp)
    app.register_blueprint(eoac_ccaa_bp)
    app.register_blueprint(eoac_pais_bp)
    app.register_blueprint(eoac_provincia_bp)
    app.register_blueprint(eoac_punt_tur_bp)
    app.register_blueprint(eoac_zona_tur_bp)

    app.register_blueprint(eoal_ccaa_bp)
    app.register_blueprint(eoal_pais_bp)

    app.register_blueprint(eoap_ccaa_bp)
    app.register_blueprint(eoap_pais_bp)
    app.register_blueprint(eoap_provincia_bp)
    app.register_blueprint(eoap_punt_tur_bp)
    app.register_blueprint(eoap_zona_tur_bp)

    app.register_blueprint(eoh_categoria_bp)
    app.register_blueprint(eoh_ccaa_bp)
    app.register_blueprint(eoh_pais_bp)
    app.register_blueprint(eoh_prov_bp)
    app.register_blueprint(eoh_punt_tur_bp)
    app.register_blueprint(eoh_zona_tur_bp)

    app.register_blueprint(etr_bp)
    
    app.register_blueprint(frontur_bp)
    # app.register_blueprint(eoac_ccaa_bp)
    # app.register_blueprint(aena_destinos_bp)

    app.register_blueprint(vivienda_turistica_prov_bp)
    app.register_blueprint(vivienda_turistica_mun_bp)
    app.register_blueprint(vivienda_turistica_ccaa_bp)

    app.register_blueprint(valores_climatologicos_viento_bp)
    app.register_blueprint(valores_climatologicos_temperatura_bp)
    app.register_blueprint(valores_climatologicos_sol_bp)
    app.register_blueprint(valores_climatologicos_precipitacion_bp)

    app.register_blueprint(turismo_receptor_prov_pais_bp)
    app.register_blueprint(turismo_receptor_mun_pais_bp)
    app.register_blueprint(turismo_receptor_ccaa_pais_bp)
    app.register_blueprint(turismo_interno_prov_ccaa_bp)
    app.register_blueprint(turismo_interno_mun_mun_bp)
    app.register_blueprint(turismo_emisor_prov_pais_bp)
    app.register_blueprint(turismo_emisor_mun_pais_bp)
    app.register_blueprint(turismo_emisor_ccaa_pais_bp)

    app.register_blueprint(transporte_terrestre_bp)

    app.register_blueprint(sepe_tipo_demandante_bp)
    app.register_blueprint(sepe_paro_registrado_bp)
    app.register_blueprint(sepe_ocupacion_bp)
    app.register_blueprint(sepe_contrato_bp)

    app.register_blueprint(seguridad_provincia_bp)
    app.register_blueprint(seguridad_ccaa_bp)

    app.register_blueprint(puertos_bp)

    app.register_blueprint(precios_eotr_tarifa_bp)
    app.register_blueprint(precios_eotr_modalidad_bp)
    app.register_blueprint(precios_eotr_ccaa_bp)
    app.register_blueprint(precios_eoh_ccaa_bp)
    app.register_blueprint(precios_eoh_categoria_bp)
    app.register_blueprint(precios_eoap_tarifa_bp)
    app.register_blueprint(precios_eoap_modalidad_bp)

    app.register_blueprint(precios_eoac_tarifa_bp)
    app.register_blueprint(precios_eoac_categoria_bp)
    app.register_blueprint(precios_alojamientos_hoteleros_bp)

    app.register_blueprint(naturaleza_superficie_protegida_bp)
    app.register_blueprint(naturaleza_areas_protegidas_ccaa_bp)

    app.register_blueprint(museo_bp)

    app.register_blueprint(ipc_ponderacion_ccaa_bp)
    app.register_blueprint(ipc_nacional_media_anual_bp)
    app.register_blueprint(ipc_nacional_bp)
    app.register_blueprint(ipc_ccaa_bp)
    app.register_blueprint(ipc_ccaa_media_anual_bp)

    app.register_blueprint(intensidad_trafico_semana_bp)
    app.register_blueprint(intensidad_trafico_mensual_bp)
    app.register_blueprint(intensidad_trafico_anual_bp)

    app.register_blueprint(ind_satisfaccion_percepcion_bp)
    app.register_blueprint(ind_satisfaccion_menciones_hotel_bp)

    app.register_blueprint(ind_rentabilidad_zona_tur_bp)
    app.register_blueprint(ind_rentabilidad_punt_tur_bp)
    app.register_blueprint(ind_rentabilidad_provincia_bp)
    app.register_blueprint(ind_rentabilidad_ccaa_bp)
    app.register_blueprint(ind_rentabilidad_categoria_bp)

    app.register_blueprint(iass_ocupacion_bp)
    app.register_blueprint(iass_negocio_bp)

    app.register_blueprint(escucha_activa_bp)

    app.register_blueprint(epa_turismo_bp)
    app.register_blueprint(epa_economia_servicios_turismo_bp)
    app.register_blueprint(epa_ccaa_bp)

    app.register_blueprint(eotr_zona_tur_bp)
    app.register_blueprint(eotr_provincia_bp)
    app.register_blueprint(eotr_pais_bp)
    app.register_blueprint(eotr_ccaa_bp)



    return app
