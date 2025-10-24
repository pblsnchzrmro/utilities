from io import BytesIO
import logging

from flask import Blueprint, request, send_file, Response, make_response
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

MIN_YEAR = 2019
MIN_MONTH = 7

file_name = 'turismo_interno_prov_ccaa'
turismo_interno_prov_ccaa_bp = Blueprint('turismo_interno_prov_ccaa', __name__)

@turismo_interno_prov_ccaa_bp.route('/TURISMO_INTERNO_PROV_CCAA_DL', methods=['GET'])
def turismo_interno_prov_ccaa_dl():

    start_year = None
    start_month = None
    end_year = None
    end_month = None

    start_year = request.args.get('desde (año)')
    start_month = request.args.get('desde (mes)')
    end_year = request.args.get('hasta (año)')
    end_month = request.args.get('hasta (mes)')
    file_type = request.args.get('Tipo de fichero')
    
    ccaa_origen = request.args.get('CCAA origen')
    province_origen = request.args.get('Provincia origen')
    ccaa_origen = "Todos" if ccaa_origen is None else ccaa_origen
    province_origen = "Todos" if province_origen is None else province_origen
    
    ccaa_destino = request.args.get('CCAA destino')
    province_destino = request.args.get('Provincia destino')
    ccaa_destino = "Todos" if ccaa_destino is None else ccaa_destino
    province_destino = "Todos" if province_destino is None else province_destino
    
    file_type = ".csv" if file_type is None else file_type


    logging.info("Parameters: {}, {}, {}, {}, {}, {}, {}, {}...".format(
        start_month, start_year, end_month, end_year, ccaa_origen, province_origen, ccaa_destino, province_destino))

    if (start_month and not start_year) or ((end_year or end_month) and not (start_year and start_month)) or not file_type in [".csv", ".xlsx"]:
        return Response(
            "Invalid parameters in the query string or body.",
            status=500
        )
    else:
        if ccaa_origen is None:
            return Response(
                "You must select a value for ccaa_origen!",
                status=500
            )
        if ccaa_destino is None:
            return Response(
                "You must select a value for ccaa_destino!",
                status=500
            )

        if province_origen is None:
            return Response(
                "You must select a value for province_origen!",
                status=500
            )
        if province_destino is None:
            return Response(
                "You must select a value for province_destino!",
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
                    "Invalid date range! The start date must be before the end date.",
                    status=500
                )

        fields = {}
        fields["CCAA_ORIGEN"] = ccaa_origen
        fields["PROVINCIA_ORIGEN"] = province_origen
        fields["CCAA_DESTINO"] = ccaa_destino
        fields["PROVINCIA_DESTINO"] = province_destino

        df = read_blob_file("/TELEFONIA_TURISMO/turismo_interno_prov_ccaa.csv", fields, start_year, start_month, end_year, end_month)
        logging.info("Telefonia turismo dataframe created, total rows {}...".format(df.shape[0]))

        df_bytes = get_df_bytes_csv(
            df) if file_type == ".csv" else get_df_bytes_xlsx(df)
        
        header = {
            "Content-Disposition": "attachment; filename=turismo_interno_prov_ccaa" + file_type}
        # header = {"Access-Control-Allow-Origin": "*","Access-Control-Expose-Headers": "content-disposition","Content-Disposition": "attachment; filename=turismo_interno_prov_ccaa" + file_type}
        mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

        logging.info("Sending file response...")
        response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'{file_name}{file_type}', mimetype=mimetype))
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
        response.headers['Content-Disposition'] = f"attachment; filename={file_name}" + file_type
        return response