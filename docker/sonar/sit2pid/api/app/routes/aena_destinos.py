import logging
from flask import Blueprint, request, send_file, Response, make_response
from io import BytesIO
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

aena_destinos_bp = Blueprint('aena_destinos', __name__)

MIN_YEAR = 2004
MIN_MONTH = 1

@aena_destinos_bp.route('/AENA_DESTINOS_DL', methods=['GET'])
def aena_destinos_dl():
    logging.info('Python HTTP trigger function processed a request.')

    start_year = None
    start_month = None
    end_year = None
    end_month = None

    aena_airport = "Todos"

    start_year = request.args.get('desde (año)')
    start_month = request.args.get('desde (mes)')
    end_year = request.args.get('hasta (año)')
    end_month = request.args.get('hasta (mes)')
    aena_airport = request.args.get('Aeropuerto AENA')
    file_type = request.args.get('Tipo de fichero')
    
    aena_airport = "Todos" if aena_airport is None else aena_airport
    file_type = ".xlsx" if file_type is None else file_type

    logging.info("Parameters: {}, {}, {}, {}, {}...".format(start_month, start_year, end_month, end_year, aena_airport))

    if (start_month and not start_year) or ((end_year or end_month) and not (start_year and start_month)) or not file_type in [".csv", ".xlsx"]:
        return Response(
                "Invalid parameters in the query string or body.",
                status=500
        )
    else:
        if start_month:
            if (int(start_month) > 12 or int(start_month) < 1):
                return Response(
                    "Month must be between 1 and 12!",
                    status=500
                )
            if int(start_year) == MIN_YEAR and int(start_month) < MIN_MONTH:
                return Response(
                    "Minimum date is " + str(MIN_YEAR) + "-" + str(MIN_MONTH).zfill(2) + "!" ,
                    status=500
                )
        if end_month:
            if (int(end_month) > 12 or int(end_month) < 1):
                return Response(
                    "Month must be between 1 and 12!",
                    status=500
                )
        if start_year:
            if int(start_year) < MIN_YEAR:
                return Response(
                    "Year must be greater than or equal to " + str(MIN_YEAR) + "!",
                    status=500
                )
        if end_year:
            if int(end_year) < MIN_YEAR:
                return Response(
                    "Year must be greater than or equal to " + str(MIN_YEAR) + "!",
                    status=500
                )
            if int(start_year + start_month.zfill(2)) > int(end_year + end_month.zfill(2)):
                return Response(
                    "Invalid range!",
                    status=500
                )

        fields = {}
        if aena_airport != "Todos" and not "Todos" in aena_airport:
            fields["AEROPUERTO_AENA"] = aena_airport

        df = read_blob_file("/AENA/aena_destinos.csv", fields, start_year, start_month, end_year, end_month)
        logging.info("Aena dataframe created, total rows {}...".format(df.size))

        df_bytes = get_df_bytes_csv(df) if file_type == ".csv" else get_df_bytes_xlsx(df)
        #header = {"Content-Disposition": "attachment; filename=aena_destinos" + file_type}
        mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

        logging.info("Sending file response...")
        response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'aena_destinos{file_type}', mimetype=mimetype))
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
        response.headers['Content-Disposition'] = "attachment; filename=aena_destinos" + file_type
        return response