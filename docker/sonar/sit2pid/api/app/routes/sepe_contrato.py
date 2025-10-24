from io import BytesIO
import logging

from flask import Blueprint, request, send_file, Response, make_response
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

MIN_YEAR = 2015
MIN_MONTH = 1

file_name = 'sepe_contrato'
sepe_contrato_bp = Blueprint('sepe_contrato', __name__)

@sepe_contrato_bp.route('/SEPE_CONTRATO_DL', methods=['GET'])
def sepe_contrato_dl():

    start_year = None
    start_month = None
    end_year = None
    end_month = None

    start_year = request.args.get('desde (año)')
    start_month = request.args.get('desde (mes)')
    end_year = request.args.get('hasta (año)')
    end_month = request.args.get('hasta (mes)')
    
    ccaa = request.args.get('CCAA')
    province = request.args.get('Provincia')
    # contract_type = request.args.get('Tipo de contrato')
    file_type = request.args.get('Tipo de fichero')

    ccaa = "Todos" if ccaa is None else ccaa
    province = "Todos" if province is None else province
    # contract_type = "Todos" if contract_type is None else contract_type
    file_type = ".csv" if file_type is None else file_type

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
        # if contract_type is None:
        #     return Response(
        #         "You must select a value for contract_type!",
        #         status=500
        #     )
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
        # fields["TIPO_CONTRATO"] = contract_type

        df = read_blob_file("/SEPE/sepe_contrato.csv", fields,
                            start_year, start_month, end_year, end_month)
        logging.info(
            "SEPE dataframe created, total rows {}...".format(df.size))

        df = df.astype({"NUM_TOT_CONTRATO": int})

        df_bytes = get_df_bytes_csv(
            df) if file_type == ".csv" else get_df_bytes_xlsx(df)
        header = {
            "Content-Disposition": "attachment; filename=sepe_contrato" + file_type}
        mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

        logging.info("Sending file response...")
        response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'{file_name}{file_type}', mimetype=mimetype))
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
        response.headers['Content-Disposition'] = f"attachment; filename={file_name}" + file_type
        return response
