from io import BytesIO
import logging

from flask import Blueprint, request, send_file, Response, make_response
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

MIN_YEAR = 2011
MIN_MONTH = 1

file_name = 'valores_climatologicos_viento'
valores_climatologicos_viento_bp = Blueprint('valores_climatologicos_viento', __name__)

@valores_climatologicos_viento_bp.route('/VALORES_CLIMATOLOGICOS_VIENTO_DL', methods=['GET'])
def valores_climatologicos_viento_dl():

    start_year = None
    start_month = None
    end_year = None
    end_month = None

    start_year = request.args.get('desde (año)')
    start_month = request.args.get('desde (mes)')
    end_year = request.args.get('hasta (año)')
    end_month = request.args.get('hasta (mes)')
    file_type = request.args.get('Tipo de fichero', '.csv')
    
    ccaa = request.args.get('CCAA', 'Todos')
    province = request.args.get('Provincia', 'Todos')


    logging.info("Parameters: {}, {}, {}, {}, {}, {}...".format(
        start_month, start_year, end_month, end_year, ccaa, province))

    if (start_month and not start_year) or ((end_year or end_month) and not (start_year and start_month)) or not file_type in [".csv", ".xlsx"]:
        return Response(
            "Invalid parameters in the query string or body.",
            status=500
        )
    else:
        if ccaa is None:
            return Response(
                "You must select a value for ccaa!",
                status=500
            )
        if province is None:
            return Response(
                "You must select a value for province!",
                status=500
            )
        if start_month:
            if (int(start_month) > 12 or int(start_month) < 1):
                return Response(
                    "Month must be between 1 and 12!",
                    status=500
                )
            if int(start_year) == MIN_YEAR and int(start_month) < MIN_MONTH:
                return Response(
                    "Minimum date is " +
                    str(MIN_YEAR) + "-" + str(MIN_MONTH).zfill(2) + "!",
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
            if int(start_year + start_month.zfill(2)) > int(end_year + end_month.zfill(2)):
                return Response(
                    "Invalid range!",
                    status=500
                )

        fields = {}
        fields["CCAA"] = ccaa
        fields["PROVINCIA"] = province

    
        df = read_blob_file("/AEMET/viento.csv", fields, start_year, start_month, end_year, end_month)
        logging.info("Viento dataframe created, total rows {}...".format(df.shape[0]))


        df_bytes = get_df_bytes_csv(
            df) if file_type == ".csv" else get_df_bytes_xlsx(df)
        header = {
            "Content-Disposition": "attachment; filename=viento" + file_type}
        # header = {'Access-Control-Allow-Origin': '*','Access-Control-Expose-Headers': 'content-disposition',"Content-Disposition": "attachment; filename=viento" + file_type}
        mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

        logging.info("Sending file response...")
        response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'{file_name}{file_type}', mimetype=mimetype))
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
        response.headers['Content-Disposition'] = f"attachment; filename={file_name}" + file_type
        return response