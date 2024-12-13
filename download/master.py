import subprocess

def run_script(script_path):
    try:
        # Ejecuta el script y espera a que termine
        subprocess.run(["python", script_path], check=True)
        print(f"Successfully executed {script_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")

if __name__ == "__main__":
    # Ruta para filtrar los videos
    filter_videos = "/home/crowdcounting/galeria-arlo/download/orionManager.py"
    # Ruta del script de descarga
    download_script = "/home/crowdcounting/galeria-arlo/download/download.py"
    # Ruta del script de procesamiento
    process_script = "/home/crowdcounting/galeria-arlo/download/processVideos.py"
    
    #Ruta para actualizar la db
    update_db = "/home/crowdcounting/galeria-arlo/download/arloManager.py"

    run_script(filter_videos)
    # Ejecutar el script de descarga
    print("Starting download script...")
    run_script(download_script)

    # Ejecutar el script de procesamiento
    print("Starting processing script...")
    run_script(process_script)

    run_script(update_db)