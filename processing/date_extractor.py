import gi
gi.require_version('GExiv2', '0.10')
from gi.repository import GExiv2
from pymediainfo import MediaInfo
import re
import logging

class DateExtractor:
    @staticmethod
    def extract_date(file_path, image_exts=None, video_exts=None):
        """
        Estrae la data da un file con gestione robusta e metodi non deprecati.
        """
        if file_path is None:
            logging.warning("extract_date chiamato con file_path None")
            return None
        
        image_exts = image_exts or []
        video_exts = video_exts or []
        suffix = file_path.suffix.lower()

        try:
            logging.debug(f"🔍 Extracting date from: {file_path} (suffix: {suffix})")

            if suffix in image_exts:
                # PRIORITÀ 1: Estrazione da EXIF per immagini
                date_result = DateExtractor._extract_from_image_metadata(file_path)
                if date_result:
                    logging.info(f"✅ Data estratta da EXIF per {file_path.name}: {date_result}")
                    return date_result
                else:
                    logging.warning(f"⚠️ Estrazione EXIF fallita per {file_path.name}")

            elif suffix in video_exts:
                # PRIORITÀ 1: Estrazione da metadata video
                date_result = DateExtractor._extract_from_video_metadata(file_path)
                if date_result:
                    logging.info(f"✅ Data estratta da metadata video per {file_path.name}: {date_result}")
                    return date_result
                else:
                    logging.warning(f"⚠️ Estrazione metadata video fallita per {file_path.name}")

            # PRIORITÀ 2: Fallback a filename parsing
            date_result = DateExtractor._extract_from_filename(file_path)
            if date_result:
                logging.info(f"✅ Data estratta da filename per {file_path.name}: {date_result}")
                return date_result
            else:
                logging.warning(f"⚠️ Estrazione filename fallita per {file_path.name}")

            # PRIORITÀ 3: Nessuna data trovata
            logging.error(f"❌ IMPOSSIBILE estrarre data per {file_path}")
            return None

        except Exception as e:
            logging.error(f"❌ Errore generale nell'estrazione data per {file_path}: {e}")
            return None

    @staticmethod
    def _extract_from_image_metadata(file_path):
        """
        Estrae data da metadata immagine usando metodi NON deprecati.
        """
        try:
            meta = GExiv2.Metadata()
            
            # Soppressione stderr per evitare warning GExiv2 inutili
            import os, sys
            stderr_fd = sys.stderr.fileno()
            devnull = os.open(os.devnull, os.O_WRONLY)
            old_stderr = os.dup(stderr_fd)
            os.dup2(devnull, stderr_fd)
            
            try:
                meta.open_path(str(file_path))
            finally:
                os.dup2(old_stderr, stderr_fd)
                os.close(old_stderr)
                os.close(devnull)

            # FIX: Usa metodi NON deprecati
            # Invece di has_tag() e get_tag_string(), usa try/except diretto
            
            # Prova diversi tag EXIF in ordine di priorità
            date_tags = [
                "Exif.Photo.DateTimeOriginal",    # Data originale (più affidabile)
                "Exif.Image.DateTime",            # Data modifica
                "Exif.Photo.DateTimeDigitized"    # Data digitalizzazione
            ]
            
            for tag in date_tags:
                try:
                    # FIX: Accesso diretto invece di metodi deprecati
                    date_str = meta[tag]
                    if date_str:
                        logging.debug(f"🏷️ Tag {tag} trovato: '{date_str}'")
                        
                        # Parse formato: "YYYY:MM:DD HH:MM:SS"
                        date_part = date_str.split(" ")[0]
                        y, m, d = date_part.split(":")
                        
                        # Validazione
                        if DateExtractor._validate_date(y, m, d):
                            result = (y, m, f"{y}{m}{d}")
                            logging.debug(f"✅ Data EXIF validata: {result}")
                            return result
                        else:
                            logging.warning(f"⚠️ Data EXIF non valida: {y}-{m}-{d}")
                            
                except (KeyError, ValueError, IndexError) as e:
                    # Tag non presente o formato errato
                    logging.debug(f"🏷️ Tag {tag} non utilizzabile: {e}")
                    continue
                except Exception as e:
                    logging.warning(f"⚠️ Errore leggendo tag {tag}: {e}")
                    continue
            
            logging.debug(f"⚠️ Nessun tag EXIF utilizzabile per {file_path}")
            return None
            
        except Exception as e:
            logging.error(f"❌ Errore lettura metadata immagine per {file_path}: {e}")
            return None

    @staticmethod  
    def _extract_from_video_metadata(file_path):
        """Estrae data da metadata video con gestione robusta."""
        try:
            media_info = MediaInfo.parse(file_path)
            
            for track in media_info.tracks:
                if track.track_type == "General":
                    # Prova diversi campi data in ordine di priorità
                    date_fields = [
                        track.encoded_date,
                        track.tagged_date,
                        track.file_last_modification_date
                    ]
                    
                    for date_str in date_fields:
                        if date_str:
                            logging.debug(f"🎬 Video date field: '{date_str}'")
                            match = re.search(r"(\d{4})-(\d{2})-(\d{2})", str(date_str))
                            if match:
                                y, m, d = match.groups()
                                if DateExtractor._validate_date(y, m, d):
                                    result = (y, m, f"{y}{m}{d}")
                                    logging.debug(f"✅ Data video validata: {result}")
                                    return result
            
            logging.debug(f"⚠️ Nessuna data video trovata per {file_path}")
            return None
            
        except Exception as e:
            logging.error(f"❌ Errore lettura metadata video per {file_path}: {e}")
            return None

    @staticmethod
    def _extract_from_filename(file_path):
        """Estrae data dal filename con pattern multipli."""
        try:
            name = file_path.name
            logging.debug(f"📄 Parsing filename: '{name}'")
            
            # Pattern multipli per diversi formati filename
            patterns = [
                r"(\d{4})[-_](\d{2})[-_](\d{2})",  # YYYY-MM-DD, YYYY_MM_DD
                r"(\d{4})(\d{2})(\d{2})",          # YYYYMMDD
                r"IMG_(\d{4})(\d{2})(\d{2})",      # IMG_YYYYMMDD
                r"MVI_(\d{4})(\d{2})(\d{2})",      # MVI_YYYYMMDD
                r"DSC_(\d{4})(\d{2})(\d{2})",      # DSC_YYYYMMDD
            ]
            
            for pattern in patterns:
                match = re.search(pattern, name)
                if match:
                    y, m, d = match.groups()
                    logging.debug(f"🎯 Pattern match: {y}-{m}-{d}")
                    
                    # Validazione data
                    if DateExtractor._validate_date(y, m, d):
                        result = (y, m, f"{y}{m}{d}")
                        logging.debug(f"✅ Data filename validata: {result}")
                        return result
                    else:
                        logging.warning(f"⚠️ Data filename non valida: {y}-{m}-{d}")
            
            logging.debug(f"⚠️ Nessun pattern filename riconosciuto per '{name}'")
            return None
            
        except Exception as e:
            logging.error(f"❌ Errore parsing filename per {file_path}: {e}")
            return None

    @staticmethod
    def _validate_date(year, month, day):
        """Valida una data estratta."""
        try:
            y, m, d = int(year), int(month), int(day)
            return (1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31)
        except (ValueError, TypeError):
            return False