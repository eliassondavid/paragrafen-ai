Nuvarande implementation följer specen med följande explicita avvikelse:

- `ingest/prop_ingest.py` accepterar `--from-date`, `--to-date` och `--rm`, men applicerar dem ännu inte i fetch-steget. Orsak: det lokala repoet saknar etablerat kontrakt för dessa filterparametrar i Riksdagens API-klient, och att gissa parameterformat vore riskabelt. Alternativ: utöka fetchern när API-parametrarna har verifierats i projektets källkontrakt.
