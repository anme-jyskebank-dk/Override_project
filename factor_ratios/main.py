from fastapi import FastAPI
from factor_ratios.endpoints import endpoints

app = FastAPI(title="Research factor ratios",
    description='''**Følgende indeholder et par grupper af endpoints til at beregne diverse faktor ratios på baggrund af resultater udregnet af Jyske Quant Global Large Cap Equities.**

                \n **Factor ratios:** Den første gruppe afleverer en tabel i json format med deskriptive 40/40 ratios for de valgte region, lande, sektorer, regionale sektorer eller
                industrier. Den første GET request skaber en tabel på ugebasis, hvor man aflevere et WeekID og får den tilhørende tabel tilbage. Det samme gør sig gældende for den
                anden GET request, hvor man i stedet aflevere det antal uger tilbage i tiden, fx 52 for et år, der ønskes data for. Begge tabeller tager yderligere tre parametre
                som input "type", "region" og "marketcap", der bruges til henholdsvis at filtrere de underliggende papir og til at vælge om faktor ratios'ne skal være marketcap vægtet.

                \n **A single factor:** Denne gruppe afleverer historik for en enkel regions, lands, sektors, regionale sektors eller industris factor ratio(s). Den eneste forskel
                fra de forrige endpoints er, at der nu skal angives et yderligere parameter nemlig "filter_str" for at filtrere yderligere i det ønskede data. Så i "Factor ratios"
                kunne outputtet være en tabel med fx alle de europæiske lande, hvor det her kan specificeres til et enkelt land fx "Denmark" eller "DNK". Yderligere er begge endpoints
                POST requests, hvilket vil sige, at der skal afleveres en request body med i kaldet. I tilfældet med den første POST skal der afleveres en liste med de faktorer fra
                Jyske Quant man ønsker faktor ratios beregning for fx ["Jyske Quant", "Momentum"]. I den anden POST skal der afleveres et dictionary med nestede lister for alle parametre
                undtagen for "weeks" og "marketcap", da disse antages at være ens ved begge kald af sammenlignings grunde. Det sidste endpoint i denne gruppe har ingen relevant brug
                for brugeren, men bruges i excel til at tjekke for antal papirer i Jyske Quant med de angivende specifikationer.

                \n **A single subfactor:** Denne gruppe afleverer historik for en enkelt Jyske Quant faktor kvintil fx "Jyske Quant" "Q4" og aflevere faktor ratioen for denne gruppe målt
                på en subfaktor eller faktor fx "Sentiment" eller "Quality. Tidligere anvendte parametre går igen nemlig "weeks" og marketcap" men derudover skal der specificeres en "factor_score",
                en "factor_score_quint" og en "subfactor_score". Ligesom i ovenstående er der en POST request, hvor der skal afleveres et dictionary med nestede lister for alle parametre
                undtagen for "weeks" og "marketcap", hvis der ønskes faktor ratios for mere end en sub faktor ad gangen.

                \n **Serviceark:** Servicearket afleverer ved angivelse af et WeekID en tabel i json format af et ugenligt dataudtræk anvendt til beregninger i ovenstående endpoints
                indeholdende 24 kolonner med diverse scores og værdier fra Jyske Quant.

                \n **Override:** Override er en PUT request, der opdaterer en parquet fil med isin's, landekoder og marketcap faktorer, der bruges til at overskrive det eksiterende
                data fra Jyske Quant. Opdatering sker via et dictionary, hvor der angives to nested dictionaries. Først et dict med "isin"'s på papirer som "keys" og "Iso" på det
                land, det pågældende papir ønskes ændret til som "value", hvor efter region også bliver opdateret. Dernæst et dict ligeledes med "isin"'s på papirer som "keys" og
                en float som "value", der angiver hvilken faktor det ønskede papirs omsætning ønskes ganget med for at ændre marketCap til den andel af aktier, der handles. 

                \n **Drop_down:** Drop_down er en GET request, der afleverer alle mulige kombinationer af typer, region og filter_str anvendt i de andre endpoints for den seneste uge i
                Jyske Quant. Denne anvendes, så slutbrugeren kun bliver tilbudt inputvariable, der kan fremskaffe resultater for.
                
                ''')

app.include_router(endpoints.router)


