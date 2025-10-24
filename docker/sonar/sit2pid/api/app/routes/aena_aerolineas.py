import logging
from flask import Blueprint, request, send_file, Response, make_response
from io import BytesIO
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

aena_aerolineas_bp = Blueprint('aena_aerolineas', __name__)


MIN_YEAR = 2004
MIN_MONTH = 1


from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

@aena_aerolineas_bp.route('/AENA_AEROLINEAS_DL', methods=['GET'])
def aena_aerolineas_dl():


    start_year = request.args.get('desde (año)')
    start_month = request.args.get('desde (mes)')
    end_year = request.args.get('hasta (año)')
    end_month = request.args.get('hasta (mes)')
    aena_airport = request.args.get('Aeropuerto AENA', 'Todos')
    aena_airline = request.args.get('Aerolínea AENA', 'Todos')
    file_type = request.args.get('Tipo de fichero', '.xlsx')
    
    logging.info(f"Parameters: {start_month}, {start_year}, {end_month}, {end_year}, {aena_airport}, {aena_airline}")

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

        fields = {
            "AEROPUERTO_AENA": aena_airport if aena_airport != "Todos" and "Todos" not in aena_airport else None,
            "AEROLINEA_AENA": aena_airline if aena_airline != "Todos" and "Todos" not in aena_airline else None
        }

        df = read_blob_file("/AENA/aena_aerolineas.csv", fields, start_year, start_month, end_year, end_month)
        logging.info(f"Aena dataframe created, total rows {df.size}")

        df_bytes = get_df_bytes_csv(df) if file_type == ".csv" else get_df_bytes_xlsx(df)
        mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

        logging.info("Sending file response...")
        response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'aena_aerolineas{file_type}', mimetype=mimetype))
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
        response.headers['Content-Disposition'] = "attachment; filename=aena_aerolineas" + file_type
        return response