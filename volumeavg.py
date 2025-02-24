import yfinance as yf
import time
from datetime import datetime, timedelta
import pytz  # To handle timezones
import telegram
import asyncio
import concurrent.futures  # For parallel processing

# Constants
TIME_FRAME_MINUTES = 3 # Running interval
MARKET_START_HOUR = 9
MARKET_START_MINUTE = 15
VOLUME_MULTIPLIER = 2  # Threshold multiplier for alerts
MIN_VOLUME_THRESHOLD = 100  # Ignore stocks with volume < 100

# Define timezone
MARKET_TIMEZONE = pytz.timezone("Asia/Kolkata")

# Define stocks and their tickers (Add up to 300 stocks here)
stocks = {
    "Orient Cement": "ORIENTCEM.NS",
    "Sunteck Realty": "SUNTECK.NS",
    "RattanIndia Power": "RTNPOWER.NS",
    "Infibeam Avenues": "INFIBEAM.NS",
    "Gabriel India": "GABRIEL.NS",
    "Mahindra Lifespaces": "MAHLIFE.NS",
    "Hindustan Construction Company": "HCC.NS",
    "Rajesh Exports": "RAJESHEXPO.NS",
    "Sundaram Finance": "SUNDARMHLD.NS",
    "EaseMyTrip": "EASEMYTRIP.NS",
    "Orient Electric": "ORIENTELEC.NS",
    "Greenpanel Industries": "GREENPANEL.NS",
    "India Glycols": "INDIAGLYCO.NS",
    "PTC India": "PTC.NS",
    "Aarti Drugs": "AARTIDRUGS.NS",
    "KSB ltd": "KSB.NS",
    "Capacit'e Infraprojects": "CAPACITE.NS",
    "Delta Corp": "DELTACORP.NS",
    "Geojit Financial Services": "GEOJITFSL.NS",
    "KCP Limited": "KCP.NS",
    "GTL Infrastructure": "GTLINFRA.NS",
    "Trident": "TRIDENT.NS",
    "Finolex Cables": "FINCABLES.NS",
    "Aarti Industries": "AARTIIND.NS",
    "NCC Limited": "NCC.NS",
    "Action Construction Equipment": "ACE.NS",
    "eClerx Services": "ECLERX.NS",
    "Indian Energy Exchange": "IEX.NS",
    "Blue Dart Express": "BLUEDART.NS",
    "Jubilant Pharmova": "JUBLPHARMA.NS",
    "BEML": "BEML.NS",
    "Granules India": "GRANULES.NS",
    "Praj Industries": "PRAJIND.NS",
    "PCBL Limited": "PCBL.NS",
    "Bombay Burmah": "BBTC.NS",
    "Tata Teleservices": "TTML.NS",
    "IndiaMART": "INDIAMART.NS",
    "RITES": "RITES.NS",
    "Finolex Industries": "FINPIPE.NS",
    "Mahanagar Gas": "MGL.NS",
    "City Union Bank": "CUB.NS",
    "Olectra Greentech": "OLECTRA.NS",
    "Zee Entertainment": "ZEEL.NS",
    "Godawari Power & Ispat": "GPIL.NS",
    "Marksans Pharma": "MARKSANS.NS",
    "CEAT": "CEATLTD.NS",
    "Reliance Infrastructure": "RELINFRA.NS",
    "PVR INOX": "PVRINOX.NS",
    "Happiest Minds Technologies": "HAPPSTMNDS.NS",
    "Raymond": "RAYMOND.NS",
    "MMTC": "MMTC.NS",
    "Alok Industries": "ALOKINDS.NS",
    "Sterling & Wilson Solar": "SWSOLAR.NS",
    "Engineers India": "ENGINERSIN.NS",
    "Syrma SGS Technology": "SYRMA.NS",
    "Gujarat Mineral Development": "GMDCLTD.NS",
    "RBL Bank": "RBLBANK.NS",
    "Quess": "QUESS.NS",
    "Network18": "NETWORK18.NS",
    "Rashtriya Chemicals and Fertilizers": "RCF.NS",
    "Tanla": "TANLA.NS",
    "KNR Constructions": "KNRCON.NS",
    "Varroc": "VARROC.NS",
    "Symphony Limited": "SYMPHONY.NS",
    "Mastek": "MASTEK.NS",
    "Voltamp Transformers": "VOLTAMP.NS",
    "Chennai Petroleum": "CHENNPETRO.NS",
    "Nazara Technologies": "NAZARA.NS",
    "Ashoka Buildcon": "ASHOKA.NS",
    "eMudhra": "EMUDHRA.NS",
    "Electrosteel Castings": "ELECTCAST.NS",
    "Texmaco Rail & Engineering": "TEXRAIL.NS",
    "Karnataka Bank": "KTKBANK.NS",
    "MOIL": "MOIL.NS",
    "South Indian Bank": "SOUTHBANK.NS",
    "national_aluminum_alloy": "NATIONALUM.NS",
    "kpit_technologies": "KPITTECH.NS",
    "adani_wilmar": "AWL.NS",
    "punjab_sind_bank": "PSB.NS",
    "jk_cement": "JKCEMENT.NS",
    "gujarat_gas": "GUJGASLTD.NS",
    "cdsl": "CDSL.NS",
    "exide_industries": "EXIDEIND.NS",
    "jupiter_wagon": "JWL.NS",
    "ircon": "IRCON.NS",
    "railtelcorp": "RAILTEL.NS",
    "ge_shipping": "GESHIP.NS",
    "hbl_engineering": "HBLENGINE.NS",
    "go_digital_insurence": "GODIGIT.NS",
    "cochin_shipyard": "COCHINSHIP.NS",
    "polycab": "POLYCAB.NS",
    "railvikas_nigam": "RVNL.NS",
    "dalmia_bharat": "DALBHARAT.NS",
    "aia_engineering": "AIAENG.NS",
    "new_india_assurance": "NIACL.NS",
    "deepak_nitrite": "DEEPAKNTR.NS",
    "tata_investment_corporation": "TATAINVEST.NS",
    "piramal_pharma": "PPLPHARMA.NS",
    "ola_electric_mobility": "OLAELEC.NS",
    "lic_housing_finance": "LICHSGFIN.NS",
    "multi_commodity_exchange": "MCX.NS",
    "anant_raj": "ANANTRAJ.NS",
    "radico_khaitan": "RADICO.NS",
    "aditya_birla_fashion_and_retail": "ABFRL.NS",
    "apollo_tyres": "APOLLOTYRE.NS",
    "indraprastha_gas": "IGL.NS",
    "vedant_fashions": "MANYAVAR.NS",
    "gland_pharma": "GLAND.NS",
    "star_health_and_allied_insurance": "STARHEALTH.NS",
    "icici_securities": "ISEC.NS",
    "sun_tv_network": "SUNTV.NS",
    "kec_international": "KEC.NS",
    "tata_chemicals": "TATACHEM.NS",
    "emami": "EMAMILTD.NS",
    "bandhan_bank": "BANDHANBNK.NS",
    "poonawalla_fincorp": "POONAWALLA.NS",
    "delhivery": "DELHIVERY.NS",
    "sumitomo_chemical_india": "SUMICHEM.NS",
    "nbcc_india": "NBCC.NS",
    "amber_enterprises_india": "AMBER.NS",
    "piramal_enterprises": "PEL.NS",
    "dr_lal_pathlabs": "LALPATHLAB.NS",
    "crompton_greaves_consumer_electricals": "CROMPTON.NS",
    "natco_pharma": "NATCOPHARM.NS",
    "kfin_technologies": "KFINTECH.NS",
    "hindustan_copper": "HINDCOPPER.NS",
    "inox_wind": "INOXWIND.NS",
    "devyani_international": "DEVYANI.NS",
    "whirlpool_india": "WHIRLPOOL.NS",
    "timken_india": "TIMKEN.NS",
    "ramco_cements": "RAMCOCEM.NS",
    "skf_india": "SKFINDIA.NS",
    "grindwell_norton": "GRINDWELL.NS",
    "atul": "ATUL.NS",
    "chambal_fertilisers": "CHAMBLFERT.NS",
    "amara_raja_energy_mobility": "ARE&M.NS",
    "alembic_pharmaceuticals": "APLLTD.NS",
    "nerolac_paints": "KANSAINER.NS",
    "cyient": "CYIENT.NS",
    "afcons_infrastructure": "AFCONS.NS",
    "tejas_networks": "TEJASNET.NS",
    "bls_international": "BLS.NS",
    "jbm_auto": "JBMA.NS",
    "castrol_india": "CASTROLIND.NS",
    "karur_vysya_bank": "KARURVYSYA.NS",
    "ramkrishna_forgings": "RKFORGE.NS",
    "reliance_power": "RPOWER.NS",
    "iifl_finance": "IIFL.NS",
    "zensar": "ZENSARTECH.NS",
    "dcm_shriram": "DCMSHRIRAM.NS",
    "redington_india": "REDINGTON.NS",
    "jindal_steel_power": "JINDALSTEL.NS",
    "bosch_india": "BOSCHLTD.NS",
    "tata_consumer_products": "TATACONSUM.NS",
    "idbi_bank": "IDBI.NS",
    "shree_cement": "SHREECEM.NS",
    "dabur": "DABUR.NS",
    "canara_bank": "CANBK.NS",
    "muthoot_finance": "MUTHOOTFIN.NS",
    "union_bank_of_india": "UNIONBANK.NS",
    "marico": "MARICO.NS",
    "pb_fintech": "POLICYBZR.NS",
    "cummins_india": "CUMMINSIND.NS",
    "hero_motocorp": "HEROMOTOCO.NS",
    "bse": "BSE.NS",
    "hindustan_petroleum": "HINDPETRO.NS",
    "nhpc_limited": "NHPC.NS",
    "suzlon": "SUZLON.NS",
    "general_insurance_corporation_of_india": "GICRE.NS",
    "oil_india": "OIL.NS",
    "indusind_bank": "INDUSINDBK.NS",
    "srf_limited": "SRF.NS",
    "adani_total_gas": "ATGL.NS",
    "torrent_power": "TORNTPOWER.NS",
    "oberoi_realty": "OBEROIRLTY.NS",
    "colgate_palmolive_india": "COLPAL.NS",
    "godrej_properties": "GODREJPROP.NS",
    "indian_bank": "INDIANB.NS",
    "sbi_card": "SBICARD.NS",
    "bharti_hexacom": "BHARTIHEXA.NS",
    "aurobindo_pharma": "AUROPHARMA.NS",
    "prestige_group": "PRESTIGE.NS",
    "uno_minda": "UNOMINDA.NS",
    "vodafone_idea": "IDEA.NS",
    "alkem_laboratories": "ALKEM.NS",
    "ashok_leyland": "ASHOKLEY.NS",
    "irctc": "IRCTC.NS",
    "phoenix_mills": "PHOENIXLTD.NS",
    "coforge": "COFORGE.NS",
    "hitachi_energy_india": "POWERINDIA.NS",
    "kalyan_jewellers_india": "KALYANKJIL.NS",
    "paytm": "PAYTM.NS",
    "bharat_forge": "BHARATFORG.NS",
    "yes_bank": "YESBANK.NS",
    "national_mineral_development_corporation": "NMDC.NS",
    "coromandel": "COROMANDEL.NS",
    "supreme_industries": "SUPREMEIND.NS",
    "lt_technology_services": "LTTS.NS",
    "berger_paints": "BERGEPAINT.NS",
    "mphasis": "MPHASIS.NS",
    "motilal_oswal_financial_services": "MOTILALOFS.NS",
    "voltas": "VOLTAS.NS",
    "pi_industries": "PIIND.NS",
    "balkrishna_industries": "BALKRISIND.NS",
    "jindal_stainless": "JSL.NS",
    "fortis_healthcare": "FORTIS.NS",
    "page_industries": "PAGEIND.NS",
    "nykaa": "NYKAA.NS",
    "central_bank_of_india": "CENTRALBK.NS",
    "petronet_lng": "PETRONET.NS",
    "madras_rubber_factory": "MRF.NS",
    "tata_communications": "TATACOMM.NS",
    "federal_bank": "FEDERALBNK.NS",
    "biocon": "BIOCON.NS",
    "container_corporation_of_india": "CONCOR.NS",
    "360_one_wam": "360ONE.NS",
    "jubilant_foodworks": "JUBLFOOD.NS",
    "aditya_birla_capital": "ABCAPITAL.NS",
    "idfc_first_bank": "IDFCFIRSTB.NS",
    "bank_of_india": "BANKINDIA.NS",
    "hudco": "HUDCO.NS",
    "au_small_finance_bank": "AUBANK.NS",
    "steel_authority_of_india": "SAIL.NS",
    "bharat_dynamics": "BDL.NS",
    "upl": "UPL.NS",
    "thermax": "THERMAX.NS",
    "glenmark_pharmaceuticals": "GLENMARK.NS",
    "apl_apollo": "APLAPOLLO.NS",
    "bank_of_maharashtra": "MAHABANK.NS",
    "apar_industries": "APARINDS.NS",
    "ipca_laboratories": "IPCALAB.NS",
    "blue_star": "BLUESTARCO.NS",
    "indian_telephone_industries": "ITI.NS",
    "kei_industries": "KEI.NS",
    "escorts_kubota": "ESCORTS.NS",
    "tata_elxsi": "TATAELXSI.NS",
    "sjvn": "SJVN.NS",
    "max_financial_services": "MFSL.NS",
    "acc": "ACC.NS"
}

# Initialize Telegram Bot
TOKEN = '7742687266:AAGcI3SbCvgf4bvsPm6EibMAgPeWDMV5MCw'  # Replace with your bot's API token
CHAT_ID = '5470123996'  # Replace with your chat ID
bot = telegram.Bot(token=TOKEN)

# Function to fetch volumes for all stocks in a batch
def fetch_volume_batch(prev_intervals, curr_start, curr_end):
    try:
        tickers = list(stocks.values())  # List of all stock tickers
        data = yf.download(tickers, period="1d", interval="1m", group_by="ticker")
        
        results = {}

        for name, ticker in stocks.items():
            if ticker not in data:
                continue

            stock_data = data[ticker]
            stock_data.index = stock_data.index.tz_convert(MARKET_TIMEZONE)

            if stock_data.empty:
                continue

            prev_volumes = []
            for start, end in prev_intervals:
                prev_data = stock_data.loc[(stock_data.index >= start) & (stock_data.index <= end)]
                prev_volumes.append(prev_data["Volume"].sum())

            curr_data = stock_data.loc[(stock_data.index >= curr_start) & (stock_data.index <= curr_end)]
            curr_volume = curr_data["Volume"].sum()
            avg_prev_volume = sum(prev_volumes) / len(prev_volumes) if prev_volumes else 0

            results[name] = (ticker, avg_prev_volume, curr_volume, prev_volumes)

        return results
    except Exception as e:
        print(f"Error fetching batch data: {e}")
        return {}

# Calculate wait time until the next interval
def get_wait_time():
    current_time = datetime.now(MARKET_TIMEZONE)
    market_open = current_time.replace(hour=MARKET_START_HOUR, minute=MARKET_START_MINUTE, second=0, microsecond=0)
    elapsed_seconds = int((current_time - market_open).total_seconds())
    seconds_into_interval = elapsed_seconds % (TIME_FRAME_MINUTES * 60)
    wait_seconds = (TIME_FRAME_MINUTES * 60) - seconds_into_interval
    return wait_seconds

# Send notification using Telegram
async def send_telegram_message(msg):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        print(f"Error sending Telegram message: {e}")

# Monitor stocks for volume spikes
async def monitor_stocks():
    while True:
        wait_time = get_wait_time()
        print(f"⌛ Waiting for {wait_time} seconds until the next interval...")
        time.sleep(wait_time + 5)  # Adding buffer time

        current_time = datetime.now(MARKET_TIMEZONE)
        current_start = current_time.replace(second=0, microsecond=0) - timedelta(minutes=TIME_FRAME_MINUTES)
        current_end = current_start + timedelta(minutes=TIME_FRAME_MINUTES - 1, seconds=59)

        prev_intervals = [
            (current_start - timedelta(minutes=i * TIME_FRAME_MINUTES), 
             current_start - timedelta(minutes=(i - 1) * TIME_FRAME_MINUTES, seconds=1))
            for i in range(1, 6)
        ]

        print(f"🔍 Checking volume from {current_start.strftime('%H:%M:%S')} to {current_end.strftime('%H:%M:%S')}")

        # Fetch volume data in parallel for efficiency
        stock_data = fetch_volume_batch(prev_intervals, current_start, current_end)

        alert_stocks = []
        for name, (ticker, avg_prev_vol, curr_vol, prev_vols) in stock_data.items():
            if avg_prev_vol >= MIN_VOLUME_THRESHOLD and curr_vol >= VOLUME_MULTIPLIER * avg_prev_vol:
                alert_stocks.append(f"{name} ({ticker})\nAvg Prev Volume: {avg_prev_vol:.0f}\nCurrent Volume: {curr_vol}\nMultiplier: {curr_vol / avg_prev_vol:.1f}x")

        if alert_stocks:
            message = "\n".join(alert_stocks)
            await send_telegram_message(f"🚨 Volume Spike Alert!\n\n{message}")
        else:
            print("✅ No volume spikes detected.")

# Run the monitoring function
if __name__ == "__main__":
    asyncio.run(monitor_stocks())
