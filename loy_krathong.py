"""Loy Krathong 2026 site-support monitor (tab "Loy Krathong 2026" under Regional Monitoring).

Site list, embedded below as (region, province code, Event Name, Event Start Date, Event End Date, Site Serving,
Site_Latitude, Site_Longitude):
  NOR1 = Site_Support_Loy_Krathong_2026_UPC1.xlsx (sheet "Site Serve_UPC1", R01-North Upper)
  NOR2 = Site_Support_Loy_Krathong_2026_U2.xlsx (sheet "Site Serve", R02-North Lower)
Province code comes from the event name/location. To change the list, edit SITES and redeploy.

A pending ticket belongs to a site when the site ID appears as a token in its CINAME or SUBJECT (same idea as
site_capacity.match_site_ids). SEVERITY SA1-SA4 (site down, red on the page) and NSA1-NSA4 (site up, light orange)
both count; every other severity is ignored.
Field-team status = Mateline Status (OWS GGS), today-only rule, same as the Focus P0 page.
"""
import logging
import re

from pending_ticket import fetch_live_rows, load_work_log, _dd_daily_extras, _safe_str
from pending_trend import bangkok_now
from mateline_status import build_mateline_status_lookup
from realtime_monitor import _parse_dt, _classify_priority

log = logging.getLogger(__name__)

SEVERITIES = {"SA1", "SA2", "SA3", "SA4", "NSA1", "NSA2", "NSA3", "NSA4"}
SEASON = ("2026-11-15", "2026-11-27")
MAIN_NIGHT = "2026-11-24"

SITES = [
    ('NOR1', 'CRI', 'Loy Krathong Chiang Rai', '2026-11-15', '2026-11-26', 'CRI0021', 19.92811, 99.81526),
    ('NOR1', 'CRI', 'Loy Krathong Chiang Rai', '2026-11-15', '2026-11-26', 'UNOEV03', 19.921911, 99.820471),
    ('NOR1', 'CRI', 'Loy Krathong Chiang Rai', '2026-11-15', '2026-11-26', 'UNOEV16', 19.921911, 99.820471),
    ('NOR1', 'PYO', 'Loy Krathong PYO', '2026-11-15', '2026-11-26', 'UNOEV34', 19.169119, 99.896277),
    ('NOR1', 'PYO', 'Loy Krathong PYO', '2026-11-15', '2026-11-26', 'UNOEV26', 19.170488, 99.897616),
    ('NOR1', 'PYO', 'Loy Krathong PYO', '2026-11-15', '2026-11-26', 'PYO0001', 19.170488, 99.897616),
    ('NOR1', 'PYO', 'Loy Krathong PYO', '2026-11-15', '2026-11-26', 'PYO0092', 19.165909, 99.900784),
    ('NOR1', 'PYO', 'Loy Krathong PYO', '2026-11-15', '2026-11-26', 'PYO7251', 19.163302, 99.89984),
    ('NOR1', 'PYO', 'Loy Krathong PYO', '2026-11-15', '2026-11-26', 'PYO8512', 19.167991, 99.898531),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Authentic Sky Lanterns (Dutch Farm)', '2026-11-24', '2026-11-25', 'CMI3017', 18.7414, 99.1963),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Authentic Sky Lanterns (Dutch Farm)', '2026-11-24', '2026-11-25', 'CMI6993', 18.72627, 99.19482),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Lanna Lantern Festival (Wat Phra That Doi Saket)', '2026-11-24', '2026-11-25', 'CMI0009', 18.87035, 99.12854),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Lanna Lantern Festival (Wat Phra That Doi Saket)', '2026-11-24', '2026-11-25', 'CMI8742', 18.87445, 99.14955),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Yee Peng Lanna International (Thudongkasathan)', '2026-11-24', '2026-11-25', 'CMI1953', 18.91004, 99.01862),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiangmai CAD Khomloy Sky Lanterns Festival', '2026-11-24', '2026-11-25', 'UNOCW03', 18.765237, 99.244852),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiangmai CAD Khomloy Sky Lanterns Festival', '2026-11-24', '2026-11-25', 'UNOCW04', 18.765163, 99.242134),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiangmai CAD Khomloy Sky Lanterns Festival', '2026-11-24', '2026-11-25', 'UNOEV18', 18.767917, 99.244769),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Voyage of Light', '2026-11-24', '2026-11-25', 'CMI3010', 19.01778, 99.30689),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Voyage of Light', '2026-11-24', '2026-11-25', 'CMI6859', 19.03809, 99.33148),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Golden Light CM Lantern Festival', '2026-11-24', '2026-11-25', 'CMI0156', 18.61638, 98.77696),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Loy Krathong (Miracle Land)', '2026-11-24', '2026-11-25', 'CMIC061', 19.133, 98.95032),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Loy Krathong (Miracle Land)', '2026-11-24', '2026-11-25', 'CMI7837', 19.12435, 98.93947),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Sky of Wishes (MUNGMANA FARM)', '2026-11-24', '2026-11-25', 'CMIA069', 18.69515, 98.81134),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Sky of Wishes (MUNGMANA FARM)', '2026-11-24', '2026-11-25', 'CMIA069', 18.6677, 98.817),
    ('NOR1', 'LPN', '2026_Loy Krathong Lamphun Lanterns Festival (Wat PhraThatHariphunchai)', '2026-11-24', '2026-11-25', 'LPN8501', 18.57603, 99.00911),
    ('NOR1', 'CMI', '2026_CMI_Loy Krathong-Wat Pa Ngio วัดป่างิ้ว - สะพานป่าแดด-ท่าวังตาล', '2026-11-23', '2026-11-25', 'CMI1645', 18.71474, 98.98782),
    ('NOR1', 'CMI', '2026_CMI_Loy Krathong-Wat Pa Ngio วัดป่างิ้ว - สะพานป่าแดด-ท่าวังตาล', '2026-11-23', '2026-11-25', 'CMI8541', 18.728438, 98.992343),
    ('NOR1', 'CMI', '2026_CMI_Loy Krathong-Wat Pa Ngio วัดป่างิ้ว - สะพานป่าแดด-ท่าวังตาล', '2026-11-23', '2026-11-25', 'UNOCW02', 18.7193, 98.987708),
    ('NOR1', 'CMI', '2026_CMI_Loy Krathong_YI PENG-Tha Phae Gate ประตูท่าแพ', '2026-11-24', '2026-11-25', 'CMI0384', 18.786971, 98.993668),
    ('NOR1', 'CMI', '2026_CMI_Loy Krathong_YI PENG-Tha Phae Gate ประตูท่าแพ', '2026-11-24', '2026-11-25', 'CMI6898', 18.78815, 98.99302),
    ('NOR1', 'CMI', '2026_CMI_Loy Krathong_YI PENG-Tha Phae Gate ประตูท่าแพ', '2026-11-24', '2026-11-25', 'CMI6924', 18.78797, 98.993805),
    ('NOR1', 'CMI', '2026_CMI_Loy Krathong_YI PENG-Tha Phae Gate ประตูท่าแพ', '2026-11-24', '2026-11-25', 'CMIA361', 18.787827, 98.995011),
    ('NOR1', 'LPG', '2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG0590', 18.2907, 99.4885),
    ('NOR1', 'LPG', '2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG7611', 18.294987, 99.494198),
    ('NOR1', 'LPG', '2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG6847', 18.288848, 99.485928),
    ('NOR1', 'LPG', '2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG6870', 18.289016, 99.491142),
    ('NOR1', 'LPG', '2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG7006', 18.28919, 99.49567),
    ('NOR1', 'LPG', '2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG7647', 18.291154, 99.491862),
    ('NOR1', 'LPG', '2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'UNOEV22', 18.289016, 99.491142),
    ('NOR1', 'CMI', '2026_Loy Krathong Chiang Mai Mae Kha Canal คลองแม่ข่า', '2026-11-24', '2026-11-25', 'CMIC006', 18.77614, 98.99774),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI0627', 18.785875, 99.002372),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI6717', 18.78427, 99.00743),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI6896', 18.78891, 99.00183),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI7171', 18.786993, 99.009533),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI0046', 18.7873, 99.00249),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI6717', 18.785193, 99.005129),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI7227', 18.78239, 99.005768),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI8815', 18.787892, 99.005292),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMIC011', 18.7823, 99.0019),
    ('NOR1', 'MHS', '2026_Loy Krathong MHS-สะพานท่าปาย ปางหมู', '2026-11-24', '2026-11-25', 'MHS0093', 19.36321, 97.96642),
    ('NOR1', 'MHS', '2026_Loy Krathong MHS-Mae Kong Bridge สะพาน แม่สะเรียง - แม่คง', '2026-11-24', '2026-11-25', 'MHS0013', 18.160965, 97.921936),
    ('NOR1', 'MHS', '2026_Loy Krathong MHS-Mae Kong Bridge สะพาน แม่สะเรียง - แม่คง', '2026-11-24', '2026-11-25', 'MHS1606', 18.15945, 97.93638),
    ('NOR1', 'MHS', '2026_Loy Krathong MHS-Mae Kong Bridge สะพาน แม่สะเรียง - แม่คง', '2026-11-24', '2026-11-25', 'MHS7673', 18.158578, 97.928297),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI0627', 18.785875, 99.002372),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI6717', 18.78427, 99.00743),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI6896', 18.78891, 99.00183),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI7171', 18.786993, 99.009533),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI7227', 18.78239, 99.005768),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMIC011', 18.7823, 99.0019),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI0010', 18.794293, 99.000774),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI1934', 18.793038, 98.999106),
    ('NOR1', 'CMI', '2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI8912', 18.796699, 98.998286),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'SKTS007', 17.02022, 99.70679),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'LNOCW05', 17.018737, 99.700784),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'LNOEV04', 17.019126, 99.703922),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'LNOEV05', 17.017644, 99.70491),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'LNOEV06', 17.016436, 99.705138),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'LNOEV09', 17.017704, 99.70175),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'SKT6738', 17.01874, 99.69708),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'SKT7121', 17.01745, 99.71069),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'SKT0140', 17.01654, 99.70972),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'SKTC002', 17.01426, 99.70981),
    ('NOR2', 'SKT', 'งานประเพณีลอยกระทง 2569 งานลอยกระทงเผาเทียนเล่นไฟ ณ.อุทยานประวัติศาสตร์สุโขทัย', '2026-11-15', '2026-11-24', 'SKT0182', 17.02042, 99.70851),
    ('NOR2', 'PSN', 'พิธีพระราชทานปริญญาบัตร มหาวิทยาลัยราชภัฏภาคเหนือตอนล่าง ณ มหาวิทยาลัยราชภัฏพิบูลสงคราม', '2026-11-24', '2026-11-27', 'LNOEV08', 16.82954, 100.207677),
    ('NOR2', 'PSN', 'พิธีพระราชทานปริญญาบัตร มหาวิทยาลัยราชภัฏภาคเหนือตอนล่าง ณ มหาวิทยาลัยราชภัฏพิบูลสงคราม', '2026-11-24', '2026-11-27', 'PSN7740', 16.82771, 100.2073),
    ('NOR2', 'PSN', 'พิธีพระราชทานปริญญาบัตร มหาวิทยาลัยราชภัฏภาคเหนือตอนล่าง ณ มหาวิทยาลัยราชภัฏพิบูลสงคราม', '2026-11-24', '2026-11-27', 'PSN7293', 16.82855, 100.2118),
    ('NOR2', 'PSN', 'พิธีพระราชทานปริญญาบัตร มหาวิทยาลัยราชภัฏภาคเหนือตอนล่าง ณ มหาวิทยาลัยราชภัฏพิบูลสงคราม', '2026-11-24', '2026-11-27', 'PSN6838', 16.82642, 100.2169),
    ('NOR2', 'PSN', 'พิธีพระราชทานปริญญาบัตร มหาวิทยาลัยราชภัฏภาคเหนือตอนล่าง ณ มหาวิทยาลัยราชภัฏพิบูลสงคราม', '2026-11-24', '2026-11-27', 'PSN1522', 16.83393, 100.2143),
    ('NOR2', 'PSN', 'พิธีพระราชทานปริญญาบัตร มหาวิทยาลัยราชภัฏภาคเหนือตอนล่าง ณ มหาวิทยาลัยราชภัฏพิบูลสงคราม', '2026-11-24', '2026-11-27', 'PSN7183', 16.83501, 100.2019),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "ยลถิ่นภูมิปัญญา ชลาธารราชนก Phitsanulok Native" อบจ.พิษณุโลก', '2026-11-20', '2026-11-24', 'PSN1947', 16.81477, 100.35041),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "ยลถิ่นภูมิปัญญา ชลาธารราชนก Phitsanulok Native" อบจ.พิษณุโลก', '2026-11-20', '2026-11-24', 'LNOCW03', 16.808053, 100.358642),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSN1726', 16.8255, 100.26451),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSN6841', 16.82286, 100.26166),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSN7254', 16.82484, 100.25991),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSN0002', 16.8186, 100.262),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSN7253', 16.81828, 100.25972),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSN7201', 16.81696, 100.26084),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSN7276', 16.81614, 100.26285),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSN7252', 16.81553, 100.25892),
    ('NOR2', 'PSN', 'งานประเพณีลอยกระทง 2569 "น่านนที วิถีแห่งศรัทธา" เทศบาลนครพิษณุโลก', '2026-11-20', '2026-11-24', 'PSNC012', 16.8133, 100.26051),
    ('NOR2', 'KPP', 'งานประเพณีลอยกระทง 2569 "แสงแห่งศรัทธา สายธาราแห่งชากังราว" จังหวัดกำแพงเพชร', '2026-11-16', '2026-11-24', 'KPP8502', 16.46736, 99.52918),
    ('NOR2', 'KPP', 'งานประเพณีลอยกระทง 2569 "แสงแห่งศรัทธา สายธาราแห่งชากังราว" จังหวัดกำแพงเพชร', '2026-11-16', '2026-11-24', 'KPP8515', 16.472, 99.52916),
    ('NOR2', 'KPP', 'งานประเพณีลอยกระทง 2569 "แสงแห่งศรัทธา สายธาราแห่งชากังราว" จังหวัดกำแพงเพชร', '2026-11-16', '2026-11-24', 'KPP0175', 16.465, 99.52864),
    ('NOR2', 'KPP', 'งานประเพณีลอยกระทง 2569 "แสงแห่งศรัทธา สายธาราแห่งชากังราว" จังหวัดกำแพงเพชร', '2026-11-16', '2026-11-24', 'KPP8513', 16.4715, 99.51861),
    ('NOR2', 'UTR', 'งานประเพณีลอยกระทง 2569 พระแล อ.ลับแล จ.อุตรดิตถ์', '2026-11-22', '2026-11-24', 'UTR8515', 17.60545, 100.04046),
    ('NOR2', 'UTR', 'งานประเพณีลอยกระทง 2569 “สายน้ำ สายใจ สืบสานวิถี ริมน่าน” เทศบาลเมืองอุตรดิตถ์', '2026-11-23', '2026-11-24', 'UTR1298', 17.6234, 100.099997),
    ('NOR2', 'UTR', 'งานประเพณีลอยกระทง 2569 “สายน้ำ สายใจ สืบสานวิถี ริมน่าน” เทศบาลเมืองอุตรดิตถ์', '2026-11-23', '2026-11-24', 'UTR7192', 17.62198, 100.09887),
    ('NOR2', 'UTR', 'งานประเพณีลอยกระทง 2569 “สายน้ำ สายใจ สืบสานวิถี ริมน่าน” เทศบาลเมืองอุตรดิตถ์', '2026-11-23', '2026-11-24', 'UTRS003', 17.62123, 100.09793),
    ('NOR2', 'UTR', 'งานประเพณีลอยกระทง 2569 “สายน้ำ สายใจ สืบสานวิถี ริมน่าน” เทศบาลเมืองอุตรดิตถ์', '2026-11-23', '2026-11-24', 'UTRS005', 17.61995, 100.0992),
    ('NOR2', 'PCB', 'ประเพณีลอยกระทง มนต์เสน่ห์แห่งศรัทธา นคราเพชรบูรณ์', '2026-11-23', '2026-11-24', 'PCB8545', 16.41734, 101.1599),
    ('NOR2', 'PCB', 'ประเพณีลอยกระทง มนต์เสน่ห์แห่งศรัทธา นคราเพชรบูรณ์', '2026-11-23', '2026-11-24', 'PCB8537', 16.42224, 101.15783),
    ('NOR2', 'PCB', 'ประเพณีลอยกระทง มนต์เสน่ห์แห่งศรัทธา นคราเพชรบูรณ์', '2026-11-23', '2026-11-24', 'PCB1763', 16.41826, 101.1633),
    ('NOR2', 'TAK', 'งานประเพณีลอยกระทงสายไหลประทีป 1,000 ดวง จังหวัดตาก', '2026-11-21', '2026-11-24', 'TAK0103', 16.8621, 99.1197),
    ('NOR2', 'TAK', 'งานประเพณีลอยกระทงสายไหลประทีป 1,000 ดวง จังหวัดตาก', '2026-11-21', '2026-11-24', 'TAK8501', 16.87019, 99.12472),
    ('NOR2', 'TAK', 'งานประเพณีลอยกระทงสายไหลประทีป 1,000 ดวง จังหวัดตาก', '2026-11-21', '2026-11-24', 'TAK6828', 16.87386, 99.12253),
    ('NOR2', 'TAK', 'งานประเพณีลอยกระทงสายไหลประทีป 1,000 ดวง จังหวัดตาก', '2026-11-21', '2026-11-24', 'TAK7648', 16.87804, 99.12152),
    ('NOR2', 'PCT', 'ประเพณีลอยกระทง มนต์เสน่ห์แสงจันทร์ มหัศจรรย์บึงสีไฟ', '2026-11-21', '2026-11-25', 'PCT0153', 16.43165, 100.3275),
    ('NOR2', 'PCT', 'ประเพณีลอยกระทง มนต์เสน่ห์แสงจันทร์ มหัศจรรย์บึงสีไฟ', '2026-11-21', '2026-11-25', 'PCT1901', 16.42795, 100.34907),
]

_TOKEN_RE = re.compile(r"[A-Z0-9]+")


def _site_ids(text, known):
    out = []
    for tok in _TOKEN_RE.findall(str(text or "").upper()):
        if tok in known and tok not in out:
            out.append(tok)
    return out


def build_response(gs_client):
    now_dt = bangkok_now()
    today = now_dt.strftime("%Y-%m-%d")
    known = {s[5].upper() for s in SITES}
    rows = fetch_live_rows(gs_client)
    try:
        work_log = load_work_log(gs_client)
    except Exception:
        log.exception("loy: work log failed")
        work_log = {}
    try:
        mateline = build_mateline_status_lookup(gs_client, today)
    except Exception:
        log.exception("loy: mateline lookup failed")
        mateline = {}
    try:
        extras = _dd_daily_extras(gs_client, today)
    except Exception:
        log.exception("loy: GGS team lookup failed")
        extras = {}
    tickets = []
    for r in rows:
        sev = str(r.get("SEVERITY", "")).strip().upper()
        if sev not in SEVERITIES:
            continue
        sites = _site_ids(r.get("CINAME"), known)
        for x in _site_ids(r.get("SUBJECT"), known):
            if x not in sites:
                sites.append(x)
        if not sites:
            continue
        tid = str(r.get("TICKETID", "")).strip()
        m = mateline.get(tid.upper()) or {}
        wl = work_log.get(tid, {})
        target = _parse_dt(r.get("TARGETFINISH"))
        tickets.append({
            "id": tid, "sites": sites, "sev": sev,
            "pr": _classify_priority(r.get("TARGETFINISH"), now_dt),
            # the sheet's own Bookmark, as is: _exclusive_bookmark_label() would turn every unnamed Bookmark into
            # "NSA3-4", which is wrong for an SA ticket whose Bookmark is something else (or blank)
            "bm": _safe_str(r.get("Bookmark")).strip() or "(ไม่มี Bookmark)",
            "ciname": _safe_str(r.get("CINAME")), "subject": _safe_str(r.get("SUBJECT")),
            "created": _safe_str(r.get("CREATIONDATE")), "target": _safe_str(r.get("TARGETFINISH")),
            "left": round((target - now_dt).total_seconds() / 3600, 1) if target else None,
            "ows": m.get("status_mateline") or "(ไม่พบใน MatelineX)", "at": m.get("mateline_at", ""),
            "team": (extras.get(tid.upper()) or {}).get("team") or _safe_str(r.get("Tech_Team")),
            "detail": wl.get("detail", ""), "group_problem": wl.get("group_problem", ""), "plan": wl.get("plan_closed_date", ""),
        })
    return {
        "generated_at": now_dt.strftime("%Y-%m-%d %H:%M"), "today": today,
        "season": SEASON, "main_night": MAIN_NIGHT, "severities": sorted(SEVERITIES),
        "sites": [{"region": s[0], "prov": s[1], "event": s[2], "start": s[3], "end": s[4], "site": s[5], "lat": s[6], "lon": s[7]} for s in SITES],
        "tickets": tickets,
    }
