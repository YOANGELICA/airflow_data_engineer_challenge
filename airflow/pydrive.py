from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotUploadedError
import os

script_dir = os.path.dirname(__file__)
credentials_dir = os.path.join(script_dir, '../secrets/credentials_module.json')

def login():
    
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_dir
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_dir)

    if gauth.credentials is None:
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
                    
    gauth.SaveCredentialsFile(credentials_dir)
    credenciales = GoogleDrive(gauth)
    return credenciales

def upload_csv(ruta_archivo,id_folder):

    credenciales = login()
    archivo = credenciales.CreateFile({'parents': [{"kind": "drive#fileLink",\
                                                            "id": id_folder}]})
    archivo['title'] = ruta_archivo.split("/")[-1]
    archivo.SetContentFile(ruta_archivo)
    archivo.Upload()
