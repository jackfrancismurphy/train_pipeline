import re

def extract_company_and_sector(data):
  lines = data.split('\n')
  results = []
  for line in lines:
    match = re.search(r"^(.*?)\s+(\w{2})\s+", line)
    if match:
      company_name, sector_code = match.groups()
      results.append((company_name, sector_code))
  return results

# Example usage:
data = """
Company NameBusiness CodeSector CodeATOC CodeGrand Central (North West)LN


Unmapped (was Anglia Railways)HS


Unmapped (was Silverlink Train Services)HP


Unmapped (was Central Trains)HG


Unmapped (was WAGN)HQ


Unmapped (was First Great Eastern)HR


JSD Rail Research & DevelopmentRR02ZZVicta Westlink Rail (defunct)PV03ZZDB Cargo ChartersFM04ZZDB Cargo FreightWA05ZZEurostarGA06ESRail Operations GroupPH07ZZDB Cargo InternationalDA08ZZFreightliner IntermodalDB09ZZSerco Rail OperationsSD10ZZFreightliner Heavy HaulDH11ZZSLC OperationsSO11[3]SOFreight Europe (defunct)PN12ZZGrand Union TrainsLF12LFEuroporte ChannelPT13ZZAlliance RailZB14ARGrand Central (North West)LN14GC[2]Network Rail (On-Track Machines)LR15LRLORAMLC16ZZHanson & Hall Rail ServicesYG17ZZSwanage RailwaySP18SPSouth Yorkshire SupertramSJ19SJTransPennine ExpressEA20TPGreater AngliaEB21LEGrand CentralEC22GCNorthern TrainsED23NTHeathrow ConnectEE24HCGreat Western RailwayEF25GWUnmapped (was First Capital Connect)EG26FCCrossCountryEH27XCEast Midlands RailwayEM28EMWest Midlands TrainsEJ29LMLondon OvergroundEK30LONetwork Rail Virtual Freight CompanyQJ31ZZWrexham and Shropshire (defunct)EI32WSElizabeth lineEX33XRDC RailPO34ZZCaledonian SleeperES35CSVintage TrainsTY36TYSeco Rail (defunct)RU37ZZCarillion Rail CTRL (Phase 1) (defunct)RQ38ZZHarscoRT39ZZBalfour Beatty RailRZ40ZZUnmapped

41

Colas RailRG42ZZAmey Fleet ServicesRE43ZZCarillion RailRB44ZZLumoLD45LDSB (Swietelsky Babcock) RailRD46ZZUnmapped

47

Unmapped

48

VolkerRailRH49ZZWest Coast RailwaysPA50WRNorth Yorkshire Moors RailwayPR51NYPre Metro OperationsPK52ZZSNCF Freight ServicesPS53ZZGB RailfreightPE54ZZHull TrainsPF55HTNexus (Tyne & Wear Metro)PG56TWUnmapped

57

Unmapped (was Advenza Freight)PI58ZZOn Route LogisticsPM59ZZScotRailHA60SRLondon North Eastern RailwayHB61GRUnmapped

62

Unmapped

63

MerseyrailHE64MEAvanti West CoastHF65VTUnmapped

66

Unmapped

67

Unmapped

68

Unmapped

69

Unmapped

70

Transport for WalesHL71AWLegge Infrastructure ServicesLG72ZZUnmapped

73

Chiltern RailwaysHO74CHUnmapped

75

Unmapped

76

Unmapped

77

Unmapped

78

c2cHT79CCSoutheasternHU80SEUnmapped (was Gatwick Express)HV81GXUnmapped (was Southern)HW82SNUnmapped

83

South Western RailwayHY84SWIsland LinesHZ85ILHeathrow ExpressHM86HXUnmapped

87

Govia Thameslink Railway (Great Northern)ET88GNGovia Thameslink Railway (Thameslink)ET88TLSouthernHW88SNLocomotive ServicesLS89LSLUL District Line - WimbledonXB90LTLUL Bakerloo LineXC91LTNetwork Rail Reserved Pathings (non-QJ)NR92ZZLUL District Line - RichmondXE93LTFfestiniog RailwayXJ94

Varamis RailMV95

Unmapped

96

Direct Rail ServicesXH97ZZInternal TestingRM98ZZUnmapped

99

Virtual European PathsEU?EU
"""
result = extract_company_and_sector(data)
for company_name, sector_code in result:
  print(f"Company Name: {company_name}, Sector Code: {sector_code}")
