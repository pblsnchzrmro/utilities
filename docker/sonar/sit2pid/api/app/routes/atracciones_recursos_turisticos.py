import logging
from flask import Blueprint, request, send_file, Response, make_response
from io import BytesIO
from common.blob_connection import read_blob_file, read_blob_file_without_anyomes, get_df_bytes_xlsx, get_df_bytes_csv



atracciones_recursos_turisticos_bp = Blueprint('atracciones_recursos_turisticos', __name__)



@atracciones_recursos_turisticos_bp.route('/ATRACCIONES_RECURSOS_TURISTICOS_DL', methods=['GET'])
def atracciones_recursos_turisticos_dl():
    logging.info('Python HTTP trigger function processed a request.')

    ccaa = None
    province = None
    
    ccaa = request.args.get('CCAA')
    province = request.args.get('Provincia')
    file_type = request.args.get('Tipo de fichero')
    file_type = ".csv" if file_type is None else file_type
    
    ccaa = "Todos" if ccaa is None else ccaa
    province = "Todos" if province is None else province

    logging.info("Parameters: {}, {}...".format(
        ccaa, province))

    fields = {}
    fields['CCAA'] = ccaa
    fields['PROVINCIA'] = province
    
    df = read_blob_file_without_anyomes("/INFORMA/atracciones_recursos_turisticos.csv", fields)
    logging.info("Informa atracciones recursos turisticos dataframe created, total rows {}...".format(df.shape[0]))

    df_bytes = get_df_bytes_csv(
        df) if file_type == ".csv" else get_df_bytes_xlsx(df)
    header = {"Content-Disposition": "attachment; filename=atracciones_recursos_turisticos" + file_type}
    # header = {'Access-Control-Allow-Origin': '*','Access-Control-Expose-Headers': 'content-disposition',"Content-Disposition": "attachment; filename=atracciones_recursos_turisticos" + file_type}
    mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"

    logging.info("Sending file response...")
    response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'atracciones_recursos_turisticos{file_type}', mimetype=mimetype))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
    response.headers['Content-Disposition'] = "attachment; filename=atracciones_recursos_turisticos" + file_type
    return response
