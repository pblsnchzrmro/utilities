from io import BytesIO
import logging

from flask import Blueprint, request, send_file, Response, make_response
from common.blob_connection import read_blob_file_without_anyomes, get_df_bytes_xlsx, get_df_bytes_csv

MIN_YEAR = 2019

file_name = 'turismo_interno_mun_mun'
turismo_interno_mun_mun_bp = Blueprint('turismo_interno_mun_mun', __name__)

@turismo_interno_mun_mun_bp.route('/TURISMO_INTERNO_MUN_MUN_DL', methods=['GET'])
def turismo_interno_mun_mun_dl():

    year = request.args.get('año') # Obligatorio
    file_type = request.args.get('Tipo de fichero', '.csv')


    logging.info("Parameters: {}...".format(
        year))
    
    if not year:
        return Response(
            "You must select a year.",
            status=500
    )
    
    if year:
        if int(year) < MIN_YEAR:
            return Response(
                "Year must be greater than or equal to " +
                str(MIN_YEAR) + "!",
                status=500
            )
        
    fields = {}
    
    df = read_blob_file_without_anyomes(f"/TELEFONIA_TURISMO/turismo_interno_mun_mun_{year}.csv", fields)
    logging.info("Telefonia turismo dataframe created, total rows {}...".format(df.shape[0]))


    df_bytes = get_df_bytes_csv(
        df) if file_type == ".csv" else get_df_bytes_xlsx(df)
    
    header = {
        "Content-Disposition": "attachment; filename=turismo_interno_mun_mun" + file_type}
    header = {"Access-Control-Allow-Origin": "*","Access-Control-Expose-Headers": "content-disposition","Content-Disposition": "attachment; filename=turismo_interno_mun_mun" + file_type}
    mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

    logging.info("Sending file response...")
    response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'{file_name}{file_type}', mimetype=mimetype))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
    response.headers['Content-Disposition'] = f"attachment; filename={file_name}" + file_type
    return response