from io import BytesIO
import logging

from flask import Blueprint, request, send_file, Response, make_response
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

MIN_YEAR = 2002
MIN_MONTH = 1

file_name = 'precios_eoh_categoria'
precios_eoh_categoria_bp = Blueprint('precios_eoh_categoria', __name__)

@precios_eoh_categoria_bp.route('/PRECIOS_EOH_CATEGORIA_DL', methods=['GET'])
def precios_eoh_categoria_dl():

    start_year = None
    start_month = None
    end_year = None
    end_month = None

    start_year = request.args.get('desde (año)')
    start_month = request.args.get('desde (mes)')
    end_year = request.args.get('hasta (año)')
    end_month = request.args.get('hasta (mes)')
    category = request.args.get('Categoría')
    file_type = request.args.get('Tipo de fichero')

    category = "Total categorías" if category is None else category
    file_type = ".xlsx" if file_type is None else file_type

    logging.info("Parameters: {}, {}, {}, {}, {}...".format(
        start_month, start_year, end_month, end_year, category))

    if (start_month and not start_year) or ((end_year or end_month) and not (start_year and start_month)) or not file_type in [".csv", ".xlsx"]:
        return Response(
            "Invalid parameters in the query string or body.",
            satus=500
        )
    else:
        if start_month:
            if (int(start_month) > 12 or int(start_month) < 1):
                return Response(
                    "Month must be between 1 and 12!",
                    satus=500
                )
            if int(start_year) == MIN_YEAR and int(start_month) < MIN_MONTH:
                return Response(
                    "Minimum date is " +
                    str(MIN_YEAR) + "-" + str(MIN_MONTH).zfill(2) + "!",
                    satus=500
                )
        if end_month:
            if (int(end_month) > 12 or int(end_month) < 1):
                return Response(
                    "Month must be between 1 and 12!",
                    satus=500
                )
        if start_year:
            if int(start_year) < MIN_YEAR:
                return Response(
                    "Year must be greater than or equal to or equal to " + str(MIN_YEAR) + "!",
                    satus=500
                )
        if end_year:
            if int(end_year) < MIN_YEAR:
                return Response(
                    "Year must be greater than or equal to or equal to " + str(MIN_YEAR) + "!",
                    satus=500
                )
            if int(start_year + start_month.zfill(2)) > int(end_year + end_month.zfill(2)):
                return Response(
                    "Invalid range!",
                    satus=500
                )

        fields = {}
        fields["CATEGORIA"] = category if category != "Total categorías" else "Total categorías"
        
        df = read_blob_file("/PRECIOS/eoh_categoria.csv", fields,
                            start_year, start_month, end_year, end_month)
        logging.info(
            "Precios EOH Categoria dataframe created, total rows {}...".format(df.size))

        df_bytes = get_df_bytes_csv(
            df) if file_type == ".csv" else get_df_bytes_xlsx(df)
        header = {
            "Content-Disposition": "attachment; filename=eoh_categoria" + file_type}
        mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

        logging.info("Sending file response...")
        response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'{file_name}{file_type}', mimetype=mimetype))
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
        response.headers['Content-Disposition'] = f"attachment; filename={file_name}" + file_type
        return response
