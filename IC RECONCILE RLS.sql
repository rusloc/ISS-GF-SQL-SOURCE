



set dev.ic_recon_rls = 
$sql$


select 
	b.*
	,g._email
from (
				with _base as (
					-- base table with all unit names (from actual data)
							select 
								_name
							from (
											select 
												v."Company" 								_name
											from public.dax__vendortransactions v
											union all
											select 
												v."Vendor Account" 
											from public.dax__vendortransactions v
											where 1=1
												and length(v."Vendor Account" ) <= 5
											union all
											select 
												c."Company" 
											from public.dax__customertransactions c
											union all
											select 
												c."Customer Account" 
											from public.dax__customertransactions c
											where 1=1
												and length(c."Customer Account") <= 5
										) b
							group by 1
						)
			-- crossjoin = get all possible combos of units; add column 'LINK' which shows two sided link (vendor - customer)
				select 
					a._name				_side_a
					,b._name			_side_b
					,a._name || '|' || b._name || '|' || b._name ||  '|' || a._name		_link
				from _base a, _base b
		) b
left join (
	-- global access users
			select 
				a	_email
			from unnest(	array[
										'NICOLA.PELLANDINI@ISS-GF.COM'
										,'JUAN.CARLOS@ISS-GF.COM'
										,'AASTHA.AGARWAL@ISS-GF.COM'
										,'KAUSTUBH.SANKULKAR@ISS-GF.COM'
										,'KHYATI.NISHAR@ISS-GF.COM'
										,'DEVANANDAN.THEKKEVEEDU@ISS-GF.COM'
										,'NIVETHA.BABU@ISS-GF.COM'
										,'HARIHARAN.IYER@ISS-GF.COM'
										,'CLAUDIO.MOZZATI@ISS-GF.COM'
										,'GIUSEPPE.CAPUTO@ISS-GF.COM'
										,'AMARPREET.DHILLON@ISS-GF.COM'
										,'ANDREA.MANDARA@ISS-GF.COM'
										,'ANTOINE.WASSEF@ISS-GF.COM'
										,'DEXTER.FONSECA@ISS-GF.COM'
										,'FLORIAN.BRAUN@ISS-GF.COM'
										,'GAYATHRI.MOKKAPATI@ISS-GF.COM'
										,'ISMAIL.QRIFA@ISS-GF.COM'
										,'KATHLEEN.CHIONG@ISS-GF.COM'
										,'KITTY.WALIA@ISS-GF.COM'
										,'LIBRADO.ECHEVARRIA@ISS-GF.COM'
										,'MADTIKA.KUEHN@ISS-GF.COM'
										,'MARIE.MORCOS@ISS-GF.COM'
										,'MELVIN.CHANG@ISS-GF.COM'
										,'NEELU.BHOJWANI@ISS-GF.COM'
										,'NICOLA.PELLANDINI@ISS-GF.COM'
										,'NIHARIKA.VERMA@ISS-GF.COM'
										,'PRASENJIT.GHOSH@ISS-GF.COM'
										,'ROMAN.BELAKOV@ISS-GF.COM'
										,'SLAVEY.DJAHOV@ISS-GF.COM'
										,'ZIBU.ZACHARIAH@ISS-GF.COM'
										,'JINESH.RAJAN@ISS-GF.COM'
										,'RIYAZ.AHMED@ISS-GF.COM'	
										,'MOHAMMAD.ABBAS@ISS-GF.COM'							
								]
					) a	
			) g 	
	on true
union all
select 
	b.*
	,l._email
from (
				with _base as (
					-- base table with all unit names (from actual data)
							select 
								_name
							from (
											select 
												v."Company" 								_name
											from public.dax__vendortransactions v
											union all
											select 
												v."Vendor Account" 
											from public.dax__vendortransactions v
											where 1=1
												and length(v."Vendor Account" ) <= 5
											union all
											select 
												c."Company" 
											from public.dax__customertransactions c
											union all
											select 
												c."Customer Account" 
											from public.dax__customertransactions c
											where 1=1
												and length(c."Customer Account") <= 5
										) b
							group by 1
						)
			-- crossjoin = get all possible combos of units; add column 'LINK' which shows two sided link (vendor - customer)
				select 
					a._name				_side_a
					,b._name			_side_b
					,a._name || '|' || b._name || '|' || b._name ||  '|' || a._name		_link
				from _base a, _base b
		) b
inner join (
			select
				upper(a.column1) 						_email
				,upper(a.column2) 						_office
			from (
	-- local access users (manually edit the list of users: if need several offices - add more lines for the same user)
								values
									('daisy.stevens@iss-gf.com', 'BEO1')
									,('ABDOULAZIZ.GUELLEH@ISS-GF.COM','DJO1')
									,('AMAC.YILMAZKARASU@ISS-GF.COM','TRO1')
									,('ANGELA.DU@ISS-GF.COM','CNO1')
									,('ANGELA.DU@ISS-GF.COM','HKO1')
									,('CALEB.LANQUAYE@ISS-GF.COM','GHOA')
									,('CANSU.SAKIZLIGIL@ISS-GF.COM','TRO1')
									,('CLAUDIO.MOZZATI@ISS-GF.COM','ITO1')
									,('DAISY.STEVENS@ISS-GF.COM','BEO1')
									,('DUANE.LAKEY@ISS-GF.COM','NAO1')
									,('DUANE.LAKEY@ISS-GF.COM','ZAO1')
									,('FRAN.AGUERA@ISS-GF.COM','ESO1')
									,('FRAN.AGUERA@ISS-GF.COM','ITO1')
									,('GODSON.OLANIYI@ISS-GF.COM','GHOA')
									,('IMANOL.CONTRERAS@ISS-GF.COM','ESO1')
									,('JANASH.JAMALSHA@ISS-GF.COM','AEO1')
									,('JANASH.JAMALSHA@ISS-GF.COM','AEO2')
									,('JANASH.JAMALSHA@ISS-GF.COM','OMO1')
									,('JOSEPH.ASALU@ISS-GF.COM','MUO1')
									,('JOSEPH.ASALU@ISS-GF.COM','NGO2')
									,('JURGEN.DEWEERDT@ISS-GF.COM','BEO1')
									,('KENIX.HO@ISS-GF.COM','HKO1')
									,('MARJORIE.SERRANO@ISS-GF.COM','DEO1')
									,('NOMVELISO.NKOHLA@ISS-GF.COM','ZAO1')
									,('PANKAJ.RAY@ISS-GF.COM','BHO1')
									,('SAIKOU.DIALLO@ISS-GF.COM','GNO1')
									,('SIRIPORN.K@ISS-GF.COM','THO1')
									,('SUBHA.KOSHY@ISS-GF.COM','AEO1')
									,('TOM.JOSEPH@ISS-GF.COM','KWO1')
									,('YESAYA.WIDJAJA@ISS-GF.COM','SGO1')
									,('ZEEBA.SHIRAZI@ISS-GF.COM','AEO1')
									,('ZEEBA.SHIRAZI@ISS-GF.COM','AEO2')
									,('ASHUTOSH.SHARMA@ISS-GF.COM','INO1')
									,('AAMER.HUSSAIN@ISS-GF.COM','NGO1')
									,('AARON.TAMAKLOE@ISS-GF.COM','GHO1')
									,('ABDOULAZIZ.GUELLEH@ISS-GF.COM','DJO1')
									,('ABDUL.ALEEM@ISS-GF.COM','AEO1')
									,('ABEL.KASSAHUN@ISS-GF.COM','ETO1')
									,('ABISH.KURIAN@ISS-GF.COM','BHO1')
									,('AHMET.GUCLU@ISS-GF.COM','TRO1')
									,('ALBERTO.ALVAREZ@ISS-GF.COM','ESO1')
									,('ALBERTO.ESTEBAN@ISS-GF.COM','ESO1')
									,('ALBERTO.ESTEBAN@ISS-GF.COM','ITO1')
									,('ALBERTO.ESTEBAN@ISS-GF.COM','BEO1')
									,('ALESSANDRO.VARGIU@ISS-GF.COM','ITO1')
									,('ALESSIA.ACHILLI@ISS-GF.COM','ITO1')
									,('ALEX.ABRAHAM@ISS-GF.COM','QAO1')
									,('ALEX.NGO@ISS-GF.COM','VNO1')
									,('ALMA.SHE@ISS-GF.COM','HKO1')
									,('AMAC.YILMAZKARASU@ISS-GF.COM','TRO1')
									,('AMAN.RUSTAGI@IN.EY.COM','ZAO1')
									,('AMAN.RUSTAGI@IN.EY.COM','AEO1')
									,('AMAN.RUSTAGI@IN.EY.COM','INO1')
									,('AMAN.RUSTAGI@IN.EY.COM','ITO1')
									,('ANDREA.BELLINAZZI@ISS-GF.COM','ITO1')
									,('ANDREA.LAVORGNA@ISS-GF.COM','ITO1')
									,('ANGELA.DU@ISS-GF.COM','HKO1')
									,('ANGELA.DU@ISS-GF.COM','CNO1')
									,('AN.HOANG@ISS-GF.COM','VNO1')
									,('ANMOL.SHARMA7@IN.EY.COM','ITO1')
									,('ANMOL.SHARMA7@IN.EY.COM','ZAO1')
									,('ANMOL.SHARMA7@IN.EY.COM','INO1')
									,('ANMOL.SHARMA7@IN.EY.COM','AEO1')
									,('ANTONIO.VESPE@ISS-GF.COM','ITO1')
									,('ARINDAM.SARKAR@ISS-GF.COM','VNO1')
									,('ARINDAM.SARKAR@ISS-GF.COM','USO1')
									,('ARINDAM.SARKAR@ISS-GF.COM','THO1')
									,('ARINDAM.SARKAR@ISS-GF.COM','MYO1')
									,('ARINDAM.SARKAR@ISS-GF.COM','SGO1')
									,('ARINDAM.SARKAR@ISS-GF.COM','SGO1')
									,('ARINDAM.SARKAR@ISS-GF.COM','INO1')
									,('AYMAN.NEKOULA@ISS-GF.COM','EGO1')
									,('AYUSH.SINGH@ISS-GF.COM','MYO1')
									,('AYUSH.SINGH@ISS-GF.COM','SGO1')
									,('AZIANAH.AUMEER@ISS-GF.COM','MUO1')
									,('BADEMBA.BARRY@ISS-GF.COM','GNO1')
									,('BEAUTY.NTONI@ISS-GF.COM','GHO1')
									,('BEHZAD.GOUDARZIAN@ISS-GF.COM','AEO1')
									,('BERT.VANSTEENKISTE@ISS-GF.COM','BEO1')
									,('BILLAY.KAMARA@ISS-GF.COM','SLO1')
									,('BINOY.PK@ISS-GF.COM','BHO1')
									,('BIYU.WU@ISS-GF.COM','ZAO1')
									,('CANSU.SAKIZLIGIL@ISS-GF.COM','TRO1')
									,('CHIRAN.MAHARAJ@ISS-GF.COM','MUO1')
									,('CLAUDIA.ABRAHAMS@ISS-GF.COM','ZAO1')
									,('CLAUDIO.MOZZATI@ISS-GF.COM','ITO1')
									,('CLAYTON.THOMPSON@ISS-GF.COM','ZAO1')
									,('CLERESA.COMBRINK@ISS-GF.COM','ZAO1')
									,('CLINTON.VELHO@ISS-GF.COM','AEO1')
									,('DANIEL.AIAH@ISS-GF.COM','SLO1')
									,('DANIEL.ALEMSEGED@ISS-GF.COM','ETO1')
									,('DARIO.SILVIO@ISS-GF.COM','MUO1')
									,('DENISE.DIFURIA@ISS-GF.COM','ITO1')
									,('DEVON.EDSON@ISS-GF.COM','ZAO1')
									,('DHIRAJ.KALRO@ISS-GF.COM','AEO1')
									,('DUANE.LAKEY@ISS-GF.COM','NAO1')
									,('DUANE.LAKEY@ISS-GF.COM','ZAO1')
									,('EDEM.KWAWUVI@ISS-GF.COM','GHO1')
									,('EDWARD.YANG@ISS-GF.COM','CNO1')
									,('ELENA.GRIENTI@ISS-GF.COM','ITO1')
									,('ELENI.GEZAHEGN@ISS-GF.COM','ETO1')
									,('ELIF.KITAY@ISS-GF.COM','TRO1')
									,('ELIF.KOCAK@ISS-GF.COM','TRO1')
									,('EPHREM.KASSAHUN@ISS-GF.COM','ETO1')
									,('ESA.ZHANG@ISS-GF.COM','CNO1')
									,('EUGENE.DUVENAGE@ISS-GF.COM','ZAO1')
									,('EVRIM.KAPLAN@ISS-GF.COM','TRO1')
									,('FABIO.ORO@ISS-GF.COM','ITO1')
									,('FAISAL.MANGALA@ISS-GF.COM','BHO1')
									,('FAIZEL.WILLIAMS@ISS-GF.COM','ZAO1')
									,('FATMA.ALSHEMMARY@ISS-GF.COM','KWO1')
									,('FEDERICA.GUGLIOTTA@ISS-GF.COM','ITO1')
									,('FILIPPO.ALPINI@ISS-GF.COM','ITO1')
									,('FRAN.AGUERA@ISS-GF.COM','ITO1')
									,('FRAN.AGUERA@ISS-GF.COM','ESO1')
									,('FRANCK.BEDIAKON@ISS-GF.COM','CIO1')
									,('FRANCK.NANA@ISS-GF.COM','CIO1')
									,('GANESH.RANE@ISS-GF.COM','INO1')
									,('GANESH.RANE@ISS-GF.COM','THO1')
									,('GANESH.RANE@ISS-GF.COM','VNO1')
									,('GANESH.RANE@ISS-GF.COM','SGO1')
									,('GANESH.RANE@ISS-GF.COM','MYO1')
									,('GANESH.RANE@ISS-GF.COM','SGO1')
									,('GEORGE.LEBBOS@ISS-GF.COM','QAO1')
									,('GEORGE.LEBBOS@ISS-GF.COM','AEO2')
									,('GEORGE.LEBBOS@ISS-GF.COM','BHO1')
									,('GEORGE.LEBBOS@ISS-GF.COM','AEO1')
									,('GEORGE.ZHOU@ISS-GF.COM','CNO1')
									,('GIORGIO.FORIN@ISS-GF.COM','ITO1')
									,('GIRISH.CG@ISS-GF.COM','THO1')
									,('GIRISH.CG@ISS-GF.COM','SGO1')
									,('GIRISH.CG@ISS-GF.COM','VNO1')
									,('GIRISH.CG@ISS-GF.COM','MYO1')
									,('GIUSEPPE.ARNOLDI@ISS-GF.COM','ZAO1')
									,('GIUSEPPE.ARNOLDI@ISS-GF.COM','NAO1')
									,('GIZEM.COBAN@ISS-GF.COM','DEO1')
									,('GLEN.FERNANDO@ISS-GF.COM','QAO1')
									,('GODSON.OLANIYI@ISS-GF.COM','GHO1')
									,('GOVARDHAN.JILLA@ISS-GF.COM','SGO1')
									,('GOVARDHAN.JILLA@ISS-GF.COM','INO1')
									,('GOVARDHAN.JILLA@ISS-GF.COM','USO1')
									,('GOVARDHAN.JILLA@ISS-GF.COM','VNO1')
									,('GOVARDHAN.JILLA@ISS-GF.COM','THO1')
									,('GOVARDHAN.JILLA@ISS-GF.COM','MYO1')
									,('GOVARDHAN.JILLA@ISS-GF.COM','SGO1')
									,('GREGORY.CAMM@ISS-GF.COM','NAO1')
									,('GUILLAUME.BUSSER@ISS-GF.COM','CIO1')
									,('HADY.SOW@ISS-GF.COM','GNO1')
									,('HARUN.SAR@ISS-GF.COM','DEO1')
									,('HESHAM.ELREFAY@ISS-GF.COM','EGO1')
									,('IKBAL.UGURLU@ISS-GF.COM','TRO1')
									,('IKER.LOIZAGA@ISS-GF.COM','ESO1')
									,('IRMA.COME@ISS-GF.COM','ZAO1')
									,('ISMAIL.YARIS@ISS-GF.COM','TRO1')
									,('JANASH.JAMALSHA@ISS-GF.COM','AEO2')
									,('JANASH.JAMALSHA@ISS-GF.COM','OMO1')
									,('JANASH.JAMALSHA@ISS-GF.COM','AEO1')
									,('JENNA.WING@ISS-GF.COM','ZAO1')
									,('JENNY.LIU@ISS-GF.COM','CNO1')
									,('JIPIL.POONATTIL@ISS-GF.COM','AEO1')
									,('JOLEEN.PILLAY@ISS-GF.COM','ZAO1')
									,('JOSEPH.ASALU@ISS-GF.COM','NGO1')
									,('JOSEPH.ASALU@ISS-GF.COM','MUO1')
									,('JURGEN.DEWEERDT@ISS-GF.COM','BEO1')
									,('KAAN.TUNCER@ISS-GF.COM','TRO1')
									,('KADIATU.CONTEH@ISS-GF.COM','SLO1')
									,('KALPESH.KATARIA@ISS-GF.COM','SGO1')
									,('KALPESH.KATARIA@ISS-GF.COM','INO1')
									,('KALPESH.KATARIA@ISS-GF.COM','USO1')
									,('KALPESH.KATARIA@ISS-GF.COM','THO1')
									,('KALPESH.KATARIA@ISS-GF.COM','VNO1')
									,('KALPESH.KATARIA@ISS-GF.COM','MYO1')
									,('KALPESH.KATARIA@ISS-GF.COM','SGO1')
									,('KENIX.HO@ISS-GF.COM','HKO1')
									,('KENNETH.REIDER@ISS-GF.COM','SLO1')
									,('KEVIN.HEFELE@ISS-GF.COM','ZAO1')
									,('KRISHNAN.KRISHNAMURTHY@ISS-GF.COM','AEO1')
									,('KRISHNAN.KRISHNAMURTHY@ISS-GF.COM','AEO2')
									,('KUBEN.PILLAY@ISS-GF.COM','ZAO1')
									,('LAWRENCE.MISQUITH@ISS-GF.COM','OMO1')
									,('LAWRENCE.MISQUITH@ISS-GF.COM','AEO2')
									,('LAWRENCE.MISQUITH@ISS-GF.COM','BHO1')
									,('LAWRENCE.MISQUITH@ISS-GF.COM','AEO1')
									,('LIZ.LEE@ISS-GF.COM','SGO1')
									,('MANAF.LATHEEF@ISS-GF.COM','BHO1')
									,('MARISKA.SMIT@ISS-GF.COM','ZAO1')
									,('MARJORIE.SERRANO@ISS-GF.COM','DEO1')
									,('MATHIAS.TRICHT@ISS-GF.COM','BEO1')
									,('MATTEO.CASABIANCA@ISS-GF.COM','AEO2')
									,('MATTEO.CASABIANCA@ISS-GF.COM','AEO1')
									,('MATTEO.PILIA@ISS-GF.COM','ITO1')
									,('MAY.HU@ISS-GF.COM','CNO1')
									,('MICHAEL.DEWIT@ISS-GF.COM','SLO1')
									,('MICHELLE.SMITH@ISS-GF.COM','ZAO1')
									,('MICHELLE.SPANGENBERG@ISS-GF.COM','ZAO1')
									,('MOHAMED.ELWAKIL@ISS-GF.COM','BHO1')
									,('MONICA.NAW@ISS-GF.COM','SGO1')
									,('MUCAHID.ZEYREK@ISS-GF.COM','TRO1')
									,('MUHAMMAD.PEEROO@ISS-GF.COM','MUO1')
									,('MUNEEF.V@ISS-GF.COM','AEO1')
									,('MUNESH.MAHARAJ@ISS-GF.COM','ZAO1')
									,('NATHIL.PILLAI@ISS-GF.COM','AEO1')
									,('NEERI.REDDY@ISS-GF.COM','ZAO1')
									,('NEETU.CHADHA@ISS-GF.COM','VNO1')
									,('NEETU.CHADHA@ISS-GF.COM','INO1')
									,('NEETU.CHADHA@ISS-GF.COM','MYO1')
									,('NEETU.CHADHA@ISS-GF.COM','SGO1')
									,('NEETU.CHADHA@ISS-GF.COM','THO1')
									,('NORMAN.H@ISS-GF.COM','MYO1')
									,('OLIVER.AGOUDAVI@ISS-GF.COM','GHO1')
									,('OLIVIER.VANREUSEL@ISS-GF.COM','CIO1')
									,('PANKAJ.RAY@ISS-GF.COM','BHO1')
									,('PAUL.KLACKERS@ISS-GF.COM','ZAO1')
									,('PAUL.OCLOO@ISS-GF.COM','GHO1')
									,('PIAK.TAN@ISS-GF.COM','INO1')
									,('PIAK.TAN@ISS-GF.COM','SGO1')
									,('PIAK.TAN@ISS-GF.COM','SGO1')
									,('PIAK.TAN@ISS-GF.COM','CNO1')
									,('PIAK.TAN@ISS-GF.COM','HKO1')
									,('PIAK.TAN@ISS-GF.COM','MYO1')
									,('PIAK.TAN@ISS-GF.COM','THO1')
									,('PIAK.TAN@ISS-GF.COM','VNO1')
									,('PIRAJ.L@ISS-GF.COM','THO1')
									,('PIYAR.ALI@ISS-GF.COM','AEO2')
									,('PIYAR.ALI@ISS-GF.COM','AEO1')
									,('PRETISH.PN@ISS-GF.COM','AEO1')
									,('RAGHAVENDRA.CHAVDA@ISS-GF.COM','INO1')
									,('RAHUL.BHOWMICK@ISS-GF.COM','SGO1')
									,('RAHUL.BHOWMICK@ISS-GF.COM','USO1')
									,('RAHUL.BHOWMICK@ISS-GF.COM','MYO1')
									,('RAHUL.BHOWMICK@ISS-GF.COM','THO1')
									,('RAHUL.BHOWMICK@ISS-GF.COM','VNO1')
									,('RAHUL.BHOWMICK@ISS-GF.COM','SGO1')
									,('RAHUL.BHOWMICK@ISS-GF.COM','INO1')
									,('RAJASURIYA.M@ISS-GF.COM','MYO1')
									,('RANY.NADER@ISS-GF.COM','AEO1')
									,('RASHMI.KULKARNI@ISS-GF.COM','USO1')
									,('RASHMI.KULKARNI@ISS-GF.COM','MYO1')
									,('RASHMI.KULKARNI@ISS-GF.COM','THO1')
									,('RASHMI.KULKARNI@ISS-GF.COM','VNO1')
									,('RASHMI.KULKARNI@ISS-GF.COM','SGO1')
									,('RASHMI.KULKARNI@ISS-GF.COM','SGO1')
									,('RASHMI.KULKARNI@ISS-GF.COM','INO1')
									,('RAVIN.FERNANDO@ISS-GF.COM','AEO1')
									,('RAVIN.FERNANDO@ISS-GF.COM','AEO2')
									,('RONALD.COLMAN@ISS-GF.COM','ZAO1')
									,('ROSSOUW.BOTHA@ISS-GF.COM','ZAO1')
									,('RYAN.CUPIDO@ISS-GF.COM','ZAO1')
									,('RYNO.DERIDDER@ISS-GF.COM','ZAO1')
									,('SAIKOU.DIALLO@ISS-GF.COM','GNO1')
									,('SAM.WATFE@ISS-GF.COM','BHO1')
									,('SANDHYA.GAEKAWAD@ISS-GF.COM','AEO1')
									,('SAURABH.UPADHYAY@ISS-GF.COM','MYO1')
									,('SAURABH.UPADHYAY@ISS-GF.COM','SGO1')
									,('SAURABH.UPADHYAY@ISS-GF.COM','VNO1')
									,('SAURABH.UPADHYAY@ISS-GF.COM','SGO1')
									,('SAURABH.UPADHYAY@ISS-GF.COM','THO1')
									,('SAURABH.UPADHYAY@ISS-GF.COM','USO1')
									,('SAURABH.UPADHYAY@ISS-GF.COM','INO1')
									,('SETH.ACHEAMPONG@ISS-GF.COM','GHO1')
									,('SETH.OFORI@ISS-GF.COM','GHO1')
									,('SEYMA.ATASOY@ISS-GF.COM','TRO1')
									,('SHAMILA.SIEBRITZ@ISS-GF.COM','ZAO1')
									,('SHARON.EISELT@ISS-GF.COM','ZAO1')
									,('SHIVAJI.PRASAD@ISS-GF.COM','VNO1')
									,('SHIVAJI.PRASAD@ISS-GF.COM','USO1')
									,('SHIVAJI.PRASAD@ISS-GF.COM','SGO1')
									,('SHIVAJI.PRASAD@ISS-GF.COM','SGO1')
									,('SHIVAJI.PRASAD@ISS-GF.COM','INO1')
									,('SHIVAJI.PRASAD@ISS-GF.COM','MYO1')
									,('SHIVAJI.PRASAD@ISS-GF.COM','THO1')
									,('SIFAR.CHEKINTAKATH@ISS-GF.COM','OMO1')
									,('SIFAR.CHEKINTAKATH@ISS-GF.COM','AEO1')
									,('SIFAR.CHEKINTAKATH@ISS-GF.COM','AEO2')
									,('SIMON.DEWAR@ISS-GF.COM','ZAO1')
									,('SIRIPORN.K@ISS-GF.COM','THO1')
									,('STANLEY.HSU@ISS-GF.COM','CNO1')
									,('SUBHA.KOSHY@ISS-GF.COM','AEO1')
									,('SUDHANSHU.BHAGWAT@ISS-GF.COM','MYO1')
									,('SUDHANSHU.BHAGWAT@ISS-GF.COM','VNO1')
									,('SUDHANSHU.BHAGWAT@ISS-GF.COM','THO1')
									,('SUDHANSHU.BHAGWAT@ISS-GF.COM','SGO1')
									,('SUDHANSHU.BHAGWAT@ISS-GF.COM','SGO1')
									,('SUDHANSHU.BHAGWAT@ISS-GF.COM','INO1')
									,('SUDHANSHU.BHAGWAT@ISS-GF.COM','USO1')
									,('SUMEET.CHHABRA@ISS-GF.COM','THO1')
									,('SYDNEY.MORTA@ISS-GF.COM','ZAO1')
									,('TANNU.SINGH1@IN.EY.COM','BEO1')
									,('TANNU.SINGH1@IN.EY.COM','HKO1')
									,('TANNU.SINGH1@IN.EY.COM','MYO1')
									,('THEEMI.MUNGROO@ISS-GF.COM','MUO1')
									,('TIAGO.PINTO-DUARTE@ISS-GF.COM','ZAO1')
									,('TOM.JOSEPH@ISS-GF.COM','KWO1')
									,('VARGHESE.KV@ISS-GF.COM','OMO1')
									,('VIDHIKAR.KHAMBEKAR@ISS-GF.COM','INO1')
									,('VIDIT.NAGPAL@ISS-GF.COM','MYO1')
									,('VIDIT.NAGPAL@ISS-GF.COM','INO1')
									,('VIDIT.NAGPAL@ISS-GF.COM','VNO1')
									,('VIDIT.NAGPAL@ISS-GF.COM','THO1')
									,('VIDIT.NAGPAL@ISS-GF.COM','SGO1')
									,('VIJAY.RAVICHANDRAN@ISS-GF.COM','QAO1')
									,('WALTER.LIEKENS@ISS-GF.COM','BEO1')
									,('WANDA.NICHOLAS@ISS-GF.COM','ZAO1')
									,('WENDY-ANN.VANDERWESTHUIZEN@ISS-GF.COM','ZAO1')
									,('WILLIETTA.MOMORIE@ISS-GF.COM','SLO1')
									,('WIM.DEBRIE@ISS-GF.COM','BEO1')
									,('YAKUBU.OMAMEGBE@ISS-GF.COM','NGO1')
									,('YANNICK.PANIER@ISS-GF.COM','MUO1')
									,('YESAYA.WIDJAJA@ISS-GF.COM','SGO1')
									,('YORDANOS.GIRMA@ISS-GF.COM','ETO1')
									,('ZEEBA.SHIRAZI@ISS-GF.COM','AEO2')
									,('ZEEBA.SHIRAZI@ISS-GF.COM','AEO1')
									,('RESHMA.AYNIPULLY@ISS-GF.COM','INO1')
									,('GIRISH.CG@ISS-GF.COM','INO1')
									,('GODSON.OLANIYI@ISS-GF.COM', 'GHO1')
									,('GODSON.OLANIYI@ISS-GF.COM', 'GHO2')
									,('JEMMA.YANG@ISS-GF.COM','CNO1')
									,('MARY.XIE@ISS-GF.COM','CNO1')
									,('FRAN.AGUERA@ISS-GF.COM','ESO1')
									,('OSCAR.SIERRA@ISS-GF.COM','ESO1')
									,('CRISTINA.JIMENEZ@ISS-GF.COM','ESO1')
									,('EDEM.KWAWUVI@ISS-GF.COM@ISS-GF.COM','GHO1')
									,('EDEM.KWAWUVI@ISS-GF.COM@ISS-GF.COM','GHO2')
									,('SUBHA.KOSHY@ISS-GF.COM','AEO2')
									,('SUBHA.KOSHY@ISS-GF.COM','AEO1')
									,('SUBHA.KOSHY@ISS-GF.COM','AEO3')
									,('SUBHA.KOSHY@ISS-GF.COM','AEH1')
									,('KRISHNAN.KRISHNAMURTHY@ISS-GF.COM','AEO2')
									,('KRISHNAN.KRISHNAMURTHY@ISS-GF.COM','AEO1')
									,('KRISHNAN.KRISHNAMURTHY@ISS-GF.COM','AEO3')
									,('KRISHNAN.KRISHNAMURTHY@ISS-GF.COM','AEH1')
									
									,('MERIYA.SAJU@ISS-GF.COM','AEO2')
									,('MERIYA.SAJU@ISS-GF.COM','AEO1')
									,('MERIYA.SAJU@ISS-GF.COM','AEO3')
									,('MERIYA.SAJU@ISS-GF.COM','AEH1')

									,('STEPHANIE.CUPIDO@ISS-GF.COM','ZAO1')
				) a
			) l
	on b._side_a = l._office
	or b._side_b = l._office


$sql$



update public.sql_source 
set _code = current_setting('dev.ic_recon_rls')
	,_updated = now()
where 1=1
	and _report = 'IC RECONCILE'
	and _page = 'RLS'
	
	
	
	
	