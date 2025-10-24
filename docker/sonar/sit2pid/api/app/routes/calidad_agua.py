import logging
from flask import Blueprint, request, send_file, Response, make_response
from io import BytesIO
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

calidad_agua_bp = Blueprint('calidad_agua', __name__)


MIN_YEAR = 2011
MIN_MONTH = 1


@calidad_agua_bp.route('/CALIDAD_AGUA_DL', methods=['GET'])
def calidad_agua_dl():
    logging.info('Python HTTP trigger function processed a request.')

    start_year = None
    start_month = None
    end_year = None
    end_month = None

    ccaa = request.args.get('CCAA')
    province = request.args.get('Provincia')

    ccaa = "Todos" if ccaa is None else ccaa
    province = "Todos" if province is None else province

    start_year = request.args.get('desde (año)')
    # start_month = request.args.get('desde (mes)')
    start_month = 1
    end_year = request.args.get('hasta (año)')
    # end_month = request.args.get('hasta (mes)')
    end_month = 1
    ccaa = request.args.get('CCAA')
    provincia = request.args.get('Provincia')
    file_type = request.args.get('Tipo de fichero')


    ccaa = "Todos" if ccaa is None else ccaa
    provincia = "Todos" if provincia is None else provincia
    file_type = ".csv" if file_type is None else file_type

    logging.info("Parameters: {}, {}, {}, {}, {}...".format(
        start_month, start_year, end_month, end_year, provincia))

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
        fields["CCAA"] = ccaa
        fields["PROVINCIA"] = provincia
        df = read_blob_file("/CALIDAD_AGUA/calidad_agua.csv", fields, start_year, start_month, end_year, end_month)
        logging.info("Calidad Agua dataframe created, total rows {}...".format(df.shape[0]))

        df = df.drop(columns=['MES'])

        df_bytes = get_df_bytes_csv(
            df) if file_type == ".csv" else get_df_bytes_xlsx(df)
        header = {
            "Content-Disposition": "attachment; filename=calidad_agua" + file_type}
        # header = {'Access-Control-Allow-Origin': '*','Access-Control-Expose-Headers': 'content-disposition',"Content-Disposition": "attachment; filename=calidad_agua" + file_type}
        mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

        logging.info("Sending file response...")
        response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'calidad_agua{file_type}', mimetype=mimetype))
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
        response.headers['Content-Disposition'] = "attachment; filename=calidad_agua" + file_type
        return response
