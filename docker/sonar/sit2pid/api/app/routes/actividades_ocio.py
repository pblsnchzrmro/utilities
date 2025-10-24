import logging
from flask import Blueprint, request, send_file, Response, make_response
from io import BytesIO
from common.blob_connection import read_blob_file, get_df_bytes_xlsx, get_df_bytes_csv

actividades_ocio_bp = Blueprint('actividades_ocio', __name__)


MIN_YEAR = 2018
MIN_MONTH = 1

@actividades_ocio_bp.route('/ACTIVIDADES_OCIO_DL', methods=['GET'])
def actividades_ocio_dl():
    start_year = request.args.get('desde (año)')
    start_month = request.args.get('desde (mes)')
    end_year = request.args.get('hasta (año)')
    end_month = request.args.get('hasta (mes)')
    file_type = request.args.get('Tipo de fichero', '.xlsx')

    ccaa = request.args.get('CCAA', 'Todos')
    province = request.args.get('Provincia', 'Todos')

    logging.info(f"Parameters: {start_month}, {start_year}, {end_month}, {end_year}, {ccaa}, {province}")

    # Validaciones
    if (start_month and not start_year) or ((end_year or end_month) and not (start_year and start_month)) or file_type not in [".csv", ".xlsx"]:
        return Response("Invalid parameters in the query string or body.", status=500)
    
    if start_month and (int(start_month) > 12 or int(start_month) < 1):
        return Response("Month must be between 1 and 12!", status=500)
    
    if start_year and int(start_year) < MIN_YEAR:
        return Response(f"Year must be greater than or equal to {MIN_YEAR}!", status=500)
    
    if end_year and int(end_year) < MIN_YEAR:
        return Response(f"Year must be greater than or equal to {MIN_YEAR}!", status=500)

    if start_year and start_month and end_year and end_month:
        if int(start_year + start_month.zfill(2)) > int(end_year + end_month.zfill(2)):
            return Response("Invalid range!", status=500)
    
    # Filtros
    fields = {"CCAA": ccaa, "PROVINCIA": province}
    
    # Leer archivo desde el blob
    df = read_blob_file("/ACTIVIDADES_OCIO/actividades_ocio.csv", fields, start_year, start_month, end_year, end_month)
    logging.info(f"Actividades ocio dataframe created, total rows {df.shape[0]}")

    # Convertir el dataframe al formato solicitado
    df_bytes = get_df_bytes_csv(df) if file_type == ".csv" else get_df_bytes_xlsx(df)
    mimetype = "application/octet-stream" if file_type == ".csv" else "application/vnd.ms-excel"
    
    # Devolver el archivo
    logging.info("Sending file response...")
    response = make_response(send_file(BytesIO(df_bytes), as_attachment=True, download_name=f'actividades_ocio{file_type}', mimetype=mimetype))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Expose-Headers'] = 'content-disposition'
    response.headers['Content-Disposition'] = "attachment; filename=actividades_ocio" + file_type
    return response