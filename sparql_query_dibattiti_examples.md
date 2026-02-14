#### ultime 100 discussioni in organi ed aula

select distinct ?seduta ?dataSeduta ?inDiscussione  ?commissione ?aula ?resoconto where {
  ?seduta a ocd:seduta; ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>; dc:date ?dataSeduta.
  ?discussione a ocd:discussione; ocd:rif_seduta ?seduta; dc:title ?inDiscussione.
  OPTIONAL{?seduta ocd:rif_organo ?organo. ?organo dc:title ?commissione}
  OPTIONAL{?seduta ocd:rif_assemblea ?assemblea. BIND("Aula" AS ?aula)}
  OPTIONAL{?seduta dc:relation ?resoconto. FILTER(REGEX(STR(?resoconto),'pdf'))}

} ORDER BY DESC(?dataSeduta) LIMIT 100 
 
		
#### Deputati intervenuti in Aula in materia di 'immigrazione' nella XVIII Legislatura

select distinct ?deputatoId ?cognome ?nome ?argomento  
?titoloSeduta ?testo  where {
  
  ?dibattito a ocd:dibattito; ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>.
  
  ?dibattito ocd:rif_discussione ?discussione.
  ?discussione ocd:rif_seduta ?seduta.
  ?seduta dc:date ?data; dc:title ?titoloSeduta.
  ?seduta ocd:rif_assemblea ?assemblea.
  
  ##titolo della discussione
  ?discussione rdfs:label ?argomento.
  FILTER(regex(?argomento,'immigrazione','i'))
    
  ##deputato intevenuto
  ?discussione ocd:rif_intervento ?intervento.
  ?intervento ocd:rif_deputato ?deputatoId; dc:relation ?testo. 
  ?deputatoId foaf:firstName ?nome; foaf:surname ?cognome .
   
} ORDER BY ?cognome ?nome ?data 


#### Interventi in Aula, nella XVII Legislatura, del deputato Ignazio Abrignani 
#### (id 302940)

select distinct ?deputatoId ?cognome ?nome ?argomento  
?titoloSeduta ?testo  where {
  
  ?dibattito a ocd:dibattito; ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>.
  
  ?dibattito ocd:rif_discussione ?discussione.
  ?discussione ocd:rif_seduta ?seduta.
  ?seduta dc:date ?data; dc:title ?titoloSeduta.
  ?seduta ocd:rif_assemblea ?assemblea.
  
  ##titolo della discussione
  ?discussione rdfs:label ?argomento.
    
  ##deputato intevenuto
  ?discussione ocd:rif_intervento ?intervento.
  ?intervento ocd:rif_deputato ?deputatoId; dc:relation ?testo. 
  ?deputatoId foaf:firstName ?nome; foaf:surname ?cognome .
  
  ##filtro con uri deputato
  #FILTER(?deputatoId=<http://dati.camera.it/ocd/deputato.rdf/d302940_17>)
  
  ##oppure filtro su nome e cognome
  FILTER(REGEX(STR(?nome),'Ignazio','i')).
  FILTER(REGEX(STR(?cognome),'Abrignani','i'))
  
  #in un determinato anno
  #FILTER(REGEX(STR(?data),'^2015','i')).

  
} ORDER BY ?cognome ?nome ?data