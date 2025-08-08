import re, os
import polars as pl, pandas as pd
from datetime import datetime
import xml.etree.ElementTree as et


class Report:
        
    def __init__(self, filepath):        
        
        assert os.path.isabs(filepath), "invalid filepath provided"
        
        filename = os.path.basename(filepath)
        
        filenamecheck = re.search(r"(PUB_){1}[^_]{1,}[_]{1}\d{4,10}(_v){0,1}",filename)
        self._isValid = True if filenamecheck is not None else False
                
        
        if 'PUB' in filename.split('_')[0]:
            self._isPrivate = True
        elif 'CNF' in filename.split('_')[0]:
            self._isPrivate = False
        else:
            self._isPrivate = None
            # TODO: throw error at this point, the file wouldn't be valid
            
        self._name = os.path.splitext(filename)[0].split('_')[1]
        self._filepath = filepath
        self._filename = filename
        if "v" in os.path.splitext(filename)[0].split('_')[-1]:
            self._version = os.path.splitext(filename)[0].split('_')[-1].replace("v","")
        else:
            self._version = None
    
    @property
    def name(self):
        return self._name
    
    @property
    def isPrivate(self):
        return self._isPrivate
    
    @property
    def isValid(self):
        return self._isValid
    
    @property
    def filepath(self):
        return self._filepath
    
    @property
    def filename(self):
        return self._filename
    
    @property
    def version(self):
        return self._version
    
    def _EnergyORLMP(self):
        # csv has the date in the first row, it's not in the table
        # example first row: CREATED AT 2025/05/02 12:31:13 FOR 2025/05/03
        with open(self.filepath, "r") as f:
            firstrow = f.readline().strip()
            ieso_date = firstrow.split(" ")[-1]
            ieso_date = datetime.strptime(ieso_date, "%Y/%m/%d").date() #get that date
            creation_dt = firstrow.split(" ")[2] + " " + firstrow.split(" ")[3]
            creation_dt = datetime.strptime(creation_dt, "%Y/%m/%d %H:%M:%S") #get that creation dt
        f.close()
        
        if self.name == "DAHourlyEnergyLMP" or self.name == "PredispHourlyEnergyLMP":
            rename = {
                "Delivery Hour":"ieso_hour",
                "Pricing Location":"pricing_location",
                "LMP":"lmp_energy",
                "Energy Loss Price":"loss_price_energy",
                "Energy Congestion Price":"congestion_price_energy",
                }
        elif self.name == "DAHourlyORLMP" or self.name == "PredispHourlyORLMP":
            rename = {
                "Delivery Hour":"ieso_hour",
                "Pricing Location":"pricing_location",
                "LMP 10S":"lmp_10S",
                "Congestion Price 10S":"congestion_price_10S",
                "LMP 10N":"lmp_10N",
                "Congestion Price 10N":"congestion_price_10N",
                "LMP 30R":"lmp_30R",
                "Congestion Price 30R":"congestion_price_30R",
                }
        elif self.name == "RealtimeEnergyLMP":
            rename = {
                "Delivery Hour":"ieso_hour",
                "Interval":"interval",
                "Pricing Location":"pricing_location",
                "LMP":"lmp_energy",
                "Energy Loss Price":"loss_price_energy",
                "Energy Congestion Price":"congestion_price_energy",
                }
        elif self.name == "RealtimeORLMP":
            rename = {
                "Delivery Hour":"ieso_hour",
                "Interval":"interval",
                "Pricing Location":"pricing_location",
                "LMP 10S":"lmp_10S",
                "Congestion Price 10S":"congestion_price_10S",
                "LMP 10N":"lmp_10N",
                "Congestion Price 10N":"congestion_price_10N",
                "LMP 30R":"lmp_30R",
                "Congestion Price 30R":"congestion_price_30R",
                }
        
        # read the rest of the csv table, rename column names, 
        # then add the ieso_date in as an additional column
        df = pl.read_csv(self.filepath, skip_rows=1)
        df = df.rename(rename)  
        df = df.with_columns([pl.lit(ieso_date).alias("ieso_date")])
        
        # add additional columns for only PD reports for version and file creation datetime
        if "Predisp" in self.name:
            df = df.with_columns(
                [pl.lit(self.version).alias("version").cast(pl.Int8)],
                [pl.lit(creation_dt).alias("file_creation_dt")]
            )
        
        return df
    
    def _DAHourlyEnergyLMP(self):      
        df = self._EnergyORLMP()
        return df
    
    def _DAHourlyORLMP(self):      
        df = self._EnergyORLMP()
        return df
    
    def _RealtimeEnergyLMP(self):      
        df = self._EnergyORLMP()
        return df
    
    def _RealtimeORLMP(self):      
        df = self._EnergyORLMP()
        return df
    
    def _PredispHourlyEnergyLMP(self):      
        df = self._EnergyORLMP()
        return df
    
    def _PredispHourlyORLMP(self):      
        df = self._EnergyORLMP()
        return df
        
    def _RealTimeIntertieLMP(self):      
        root = et.parse(self.filepath).getroot()
        ns = {'ieso': 'http://www.ieso.ca/schema'}
        data = []
        
        ieso_date = root.find('.//ieso:DeliveryDate', ns).text
        ieso_hour = root.find('.//ieso:DeliveryHour', ns).text
        
        for a in root.findall('.//ieso:IntertieLMPrice', ns):
            intertie_name = a.find('.//ieso:IntertiePLName', ns).text
            
            for i in range(1,13):
                interval = a.find(f'.//ieso:Components[1]/ieso:IntervalLMP[{i}]/ieso:Interval', ns).text
                lmp = a.find(f'.//ieso:Components[1]/ieso:IntervalLMP[{i}]/ieso:LMP', ns).text
                lossjpg = a.find(f'.//ieso:Components[2]/ieso:IntervalLMP[{i}]/ieso:LMP', ns).text
                congestion_engy = a.find(f'.//ieso:Components[3]/ieso:IntervalLMP[{i}]/ieso:LMP', ns).text
                congestion_ext = a.find(f'.//ieso:Components[4]/ieso:IntervalLMP[{i}]/ieso:LMP', ns).text
                nisl = a.find(f'.//ieso:Components[5]/ieso:IntervalLMP[{i}]/ieso:LMP', ns).text
                
                data_line = {
                    "ieso_date":ieso_date,
                    "ieso_hour":ieso_hour,
                    "interval":interval,
                    "pricing_location":intertie_name,
                    "lmp_intertie":lmp,
                    "loss_price_energy":lossjpg,
                    "congestion_price_energy":congestion_engy,
                    "congestion_price_external":congestion_ext,
                    "nisl_price":nisl,
                    }
                
                data.append(data_line)
        
        df = pl.DataFrame(data)
        df = df.with_columns(
            pl.col("ieso_date").cast(pl.Date),
            pl.col("ieso_hour").cast(pl.Int8),
            pl.col("interval").cast(pl.Int8),
            pl.col("lmp_intertie").cast(pl.Float64),
            pl.col("loss_price_energy").cast(pl.Float64),
            pl.col("congestion_price_energy").cast(pl.Float64),
            pl.col("congestion_price_external").cast(pl.Float64),
            pl.col("nisl_price").cast(pl.Float64),
        )
        
        return df
    
    def _RealtimeOntarioZonalPrice(self):      
        root = et.parse(self.filepath).getroot()
        ns = {'ieso': 'http://www.ieso.ca/schema'}
        data = []
        
        # example text: For 2025-07-12 - Hour 21
        # this is really inconsistent. Should be just a date in this xml element.
        delivery = root.find('.//ieso:DeliveryDate', ns).text
        ieso_date = delivery.split(" ")[1]
        ieso_hour = delivery.split(" ")[-1]
        
        for i in range(1,13):
            interval = i
            zonal = root.find(f'.//ieso:RealTimePriceComponents[1]/ieso:OntarioZonalPriceInterval{i}/ieso:Interval{i}', ns).text
            lossjpg = root.find(f'.//ieso:RealTimePriceComponents[2]/ieso:OntarioZonalPriceInterval{i}/ieso:Interval{i}', ns).text
            congestion = root.find(f'.//ieso:RealTimePriceComponents[3]/ieso:OntarioZonalPriceInterval{i}/ieso:Interval{i}', ns).text
            
            data_line = {
                "ieso_date":ieso_date,
                "ieso_hour":ieso_hour,
                "interval":interval,
                "zonal_price":zonal,
                "loss_price_energy":lossjpg,
                "congestion_price_energy":congestion,
                }
            
            data.append(data_line)
        
        df = pl.DataFrame(data)
        df = df.with_columns(
            pl.col("ieso_date").cast(pl.Date),
            pl.col("ieso_hour").cast(pl.Int8),
            pl.col("interval").cast(pl.Int8),
            pl.col("zonal_price").cast(pl.Float64),
            pl.col("loss_price_energy").cast(pl.Float64),
            pl.col("congestion_price_energy").cast(pl.Float64)
        )
        
        return df
    
    
    def _DAHourlyOntarioZonalPrice(self): 
        root = et.parse(self.filepath).getroot()
        ns = {'ieso': 'http://www.ieso.ca/schema'}
        data = []
                    
        ieso_date = root.find('.//ieso:DeliveryDate', ns).text
                
        for a in root.findall('.//ieso:HourlyPriceComponents', ns):
            ieso_hour = a.find('.//ieso:PricingHour', ns).text
            zonal = a.find('.//ieso:ZonalPrice', ns).text
            lossjpg = a.find('.//ieso:LossPriceCapped', ns).text
            congestion = a.find('.//ieso:CongestionPriceCapped', ns).text
            
            data_line = {
                "ieso_date":ieso_date,
                "ieso_hour":ieso_hour,
                "zonal_price":zonal,
                "loss_price_energy":lossjpg,
                "congestion_price_energy":congestion,
                }
            
            data.append(data_line)
        
        df = pl.DataFrame(data)
        df = df.with_columns(
            pl.col("ieso_date").cast(pl.Date),
            pl.col("ieso_hour").cast(pl.Int8),
            pl.col("zonal_price").cast(pl.Float64),
            pl.col("loss_price_energy").cast(pl.Float64),
            pl.col("congestion_price_energy").cast(pl.Float64)
        )
        
        return df 
    
    def _GenOutputCapability(self): 
        root = et.parse(self.filepath).getroot()
        ns = {'ieso': 'http://www.theIMO.com/schema'}
        data = []
        
        ieso_date = root.find('.//ieso:Date', ns).text
                        
        for a in root.findall('.//ieso:Generator', ns):
            generator = a.find('.//ieso:GeneratorName', ns).text
            fuel_type = a.find('.//ieso:FuelType', ns).text
            
            hours_count = len(a.findall('.//ieso:Outputs/ieso:Output', ns))
            
            for i in range(1,hours_count+1):
                
                output = e.text if (e := a.find(f'.//ieso:Outputs/ieso:Output[{i}]/ieso:EnergyMW', ns)) is not None else None
                capability = e.text if (e := a.find(f'.//ieso:Capabilities/ieso:Capability[{i}]/ieso:EnergyMW', ns)) is not None else None
                capacity = e.text if (e := a.find(f'.//ieso:Capacities/ieso:AvailCapacity[{i}]/ieso:EnergyMW', ns)) is not None else None
                                
                data_line = {
                    "ieso_date":ieso_date,
                    "ieso_hour":i,
                    "generator":generator,
                    "fuel_type":fuel_type,
                    "output":output,
                    "capability":capability,
                    "capacity":capacity,
                    }
                
                data.append(data_line)
        

        df = pl.DataFrame(data)
        df = df.with_columns(
            pl.col("ieso_date").cast(pl.Date),
            pl.col("ieso_hour").cast(pl.Int8),
            pl.col("output").cast(pl.Int32),
            pl.col("capability").cast(pl.Int32),
            pl.col("capacity").cast(pl.Int32)
        )
        
        return df 
           
        
    def parse(self):    
        if hasattr(self, f'_{self.name}'):
            return getattr(self, f'_{self.name}')()
        else:
            print("No such method!")
