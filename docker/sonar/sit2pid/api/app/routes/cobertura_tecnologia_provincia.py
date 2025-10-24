import logging
from flask import Blueprint, request, send_file, Response, make_response
from io import BytesIO
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv
import numpy as np

cobertura_tecnologia_provincia_bp = Blueprint('cobertura_tecnologia_provincia', __name__)


ccaa_prov = {
	"Andalucía": ["Almería", "Cádiz", "Córdoba", "Granada", "Huelva", "Jaén", "Málaga", "Sevilla"],
	"Aragón": ["Huesca", "Teruel", "Zaragoza"],
	"Asturias, Principado de": ["Asturias"],
	"Balears, Illes": ["Balears, Illes"],
	"Canarias": ["Palmas, Las", "Santa Cruz de Tenerife"],
	"Cantabria": ["Cantabria"],
	"Castilla y León": ["Ávila", "Burgos", "León", "Palencia", "Salamanca", "Segovia", "Soria", "Valladolid", "Zamora"],
	"Castilla La Mancha": ["Albacete", "Ciudad Real", "Cuenca", "Guadalajara", "Toledo"],
	"Cataluña": ["Barcelona", "Girona", "Lleida", "Tarragona"],
	"Comunitat Valenciana": ["Alicante/Alacant", "Castellón/Castelló", "Valencia/València"],
	"Extremadura": ["Badajoz", "Cáceres"],
	"Galicia": ["Coruña, A", "Lugo", "Ourense", "Pontevedra"],
	"Madrid, Comunidad de": ["Madrid"],
	"Murcia, Región de": ["Murcia"],
	"Navarra, Comunidad Foral de": ["Navarra"],
	"País Vasco": ["Araba/Álava", "Gipuzkoa", "Bizkaia"],
	"Rioja, La": ["Rioja, La"],
	"Ceuta": ["Ceuta"],
	"Melilla": ["Melilla"]
}

MIN_YEAR = 2013
MIN_MONTH = 6



@cobertura_tecnologia_provincia_bp.route('/COBERTURA_TECNOLOGIA_PROVINCIA_DL', methods=['GET'])
def cobertura_tecnologia_provincia_dl():
    logging.info('Python HTTP trigger function processed a request.')

    start_year = None
    start_month = None
    end_year = None
    end_month = None

    province = "Todos"
    ccaa = "Todos"

    start_year = request.args.get('desde (año)')
    # start_month = request.args.get('desde (mes)')
    start_month = 6
    end_year = request.args.get('hasta (año)')
    # end_month = request.args.get('hasta (mes)')
    end_month = 6
    province = request.args.get('Provincia')
    ccaa = request.args.get('CCAA')
    file_type = request.args.get('Tipo de fichero')

    province = "Todos" if province is None else province
    ccaa = "Todos" if ccaa is None else ccaa
    file_type = ".xlsx" if file_type is None else file_type

    logging.info("Parameters: {}, {}, {}, {}, {}, {}...".format(
        start_month, start_year, end_month, end_year, province, ccaa))

    if (end_year and not start_year) or not file_type in [".csv", ".xlsx"]:
        return Response(
            "Invalid parameters in the query string or body.",
            status=500
        )
    else:
        if end_month:
            if (int(end_month) > 12 or int(end_month) < 1):
                return Response(
                    "Month must be between 1 and 12!",
                    status=500
                )
        if start_year:
            if int(start_year) < MIN_YEAR:
                return Response(
                    "Year must be greater than or equal to " +
                    str(MIN_YEAR) + "!",
                    status=500
                )
        if end_year:
            if int(end_year) < MIN_YEAR:
                return Response(
                    "Year must be greater than or equal to " +
                    str(MIN_YEAR) + "!",
                    status=500
                )
            if int(start_year) > int(end_year):
                return Response(
                    "Invalid range!",
                    status=500
                )

        fields = {}
        if province != "Todos" and not "Todos" in province:
            fields["PROVINCIA"] = province
        else:
            if ccaa == "Todos" or "Todos" in ccaa:
                provs = [x for x in ccaa_prov.values()]
                fields["PROVINCIA"] = [item for sublist in provs for item in sublist]
            else:
                fields["PROVINCIA"] = ccaa_prov[ccaa]

        if ccaa != "Todos" and not "Todos" in ccaa:
            fields["CCAA"] = ccaa
        else:
            fields["CCAA"] = list(ccaa_prov.keys())

        df = read_blob_file("/COBERTURA/cobertura_provincia_tecnologia.csv", fields,
                            start_year, start_month, end_year, end_month)

        logging.info(
            "COBERTURA dataframe created, total rows {}...".format(df.shape[0]))

        df = df.drop(columns=['MES'])
        df.rename(columns = {'FTTH':'Fibra (FTTH)', 'HFC':'Cable (HFC)'}, inplace = True)
        cols = df.columns.values.tolist()
        df[cols] = df[cols].replace({'0':np.nan, 0:np.nan})
        df = df.dropna(axis=1, how='all')
        df_bytes = get_df_bytes_csv(
            df) if file_type == ".csv" else get_df_bytes_xlsx(df)
        header = {
            "Content-Disposition": "attachment; filename=cobertura_provincia_tecnologiaa" + file_type}
        
        mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

        logging.info("Sending file response...")
        response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'cobertura_provincia_tecnologiaa{file_type}', mimetype=mimetype))
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
        response.headers['Content-Disposition'] = "attachment; filename=cobertura_provincia_tecnologiaa" + file_type
        return response
