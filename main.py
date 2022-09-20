"""
Pipeline that converts the data from https://notarissennetwerk.nl into RDF.

Built upon an adapted version of RDFAlchemy for Python (3.7+). Install with:

```bash
pip install git+https://github.com/LvanWissen/RDFAlchemy.git
```

Questions:
    Leon van Wissen (l.vanwissen@uva.nl)

"""

import datetime
import json
import re
import urllib
from itertools import count
import calendar

from unidecode import unidecode

import requests

import rdflib
from rdflib import Dataset, ConjunctiveGraph, Graph, URIRef, Literal, XSD, Namespace, RDFS, BNode, OWL, SKOS
from rdfalchemy import rdfSubject, rdfMultiple, rdfSingle

create = Namespace("https://data.create.humanities.uva.nl/")
schema = Namespace("http://schema.org/")
sem = Namespace("http://semanticweb.cs.vu.nl/2009/11/sem/")
bio = Namespace("http://purl.org/vocab/bio/0.1/")
foaf = Namespace("http://xmlns.com/foaf/0.1/")
void = Namespace("http://rdfs.org/ns/void#")
dcterms = Namespace("http://purl.org/dc/terms/")
saa = Namespace("https://data.goldenagents.org/datasets/SAA/ontology/")
pnv = Namespace('https://w3id.org/pnv#')
rel = Namespace("http://purl.org/vocab/relationship/")

rdflib.graph.DATASET_DEFAULT_GRAPH_ID = create

ns = Namespace("https://data.create.humanities.uva.nl/id/notarissennetwerk/")

nsPerson = Namespace(
    "https://data.create.humanities.uva.nl/id/notarissennetwerk/person/")
nsPersonName = Namespace(
    "https://data.create.humanities.uva.nl/id/notarissennetwerk/personname/")
nsEvent = Namespace(
    "https://data.create.humanities.uva.nl/id/notarissennetwerk/event/")
nsEventType = Namespace(
    "https://data.create.humanities.uva.nl/id/notarissennetwerk/eventtype/")
nsRole = Namespace(
    "https://data.create.humanities.uva.nl/id/notarissennetwerk/role/")
nsAddress = Namespace(
    "https://data.create.humanities.uva.nl/id/notarissennetwerk/address/")
nsOccupation = Namespace(
    "https://data.create.humanities.uva.nl/id/notarissennetwerk/occupation/")
nsPlace = Namespace(
    "https://data.create.humanities.uva.nl/id/notarissennetwerk/place/")

with open('data/name2adamlink.json') as infile:
    name2adamlink = json.load(infile)

with open('data/place2tgn.json') as infile:
    place2tgn = json.load(infile)

with open('data/place2ecartico.json') as infile:
    place2ecartico = json.load(infile)

with open('data/notarissenEAD.json') as infile:
    notarissenEAD = json.load(infile)


class Entity(rdfSubject):
    rdf_type = URIRef('urn:entity')

    label = rdfMultiple(RDFS.label)
    name = rdfMultiple(schema.name)
    alternateName = rdfMultiple(schema.alternateName)
    description = rdfMultiple(schema.description)

    mainEntityOfPage = rdfSingle(schema.mainEntityOfPage)
    sameAs = rdfMultiple(OWL.sameAs)

    disambiguatingDescription = rdfSingle(schema.disambiguatingDescription)

    depiction = rdfSingle(foaf.depiction)
    subjectOf = rdfMultiple(schema.subjectOf)
    about = rdfSingle(schema.about)
    url = rdfSingle(schema.url)

    inDataset = rdfSingle(void.inDataset)

    closeMatch = rdfSingle(SKOS.closeMatch)
    identifier = rdfMultiple(schema.identifier)


class CreativeWork(Entity):
    rdf_type = schema.CreativeWork

    publication = rdfMultiple(schema.publication)
    author = rdfMultiple(schema.author)

    text = rdfSingle(schema.text)

    mainEntity = rdfSingle(schema.mainEntity)


class InventoryBook(CreativeWork):
    rdf_type = schema.Book


class DatasetClass(Entity):

    # db = ConjunctiveGraph

    rdf_type = void.Dataset, schema.Dataset

    # title = rdfMultiple(dcterms.title)
    description = rdfMultiple(schema.description)
    creator = rdfMultiple(schema.creator)
    publisher = rdfMultiple(schema.publisher)
    contributor = rdfMultiple(schema.contributor)
    # source = rdfSingle(dcterms.source)
    isBasedOn = rdfSingle(schema.isBasedOn)
    # date = rdfSingle(dcterms.date)
    dateCreated = rdfSingle(schema.dateCreated)
    dateModified = rdfSingle(schema.dateModified)
    datePublished = rdfSingle(schema.datePublished)
    # created = rdfSingle(dcterms.created)
    # issued = rdfSingle(dcterms.issued)
    # modified = rdfSingle(dcterms.modified)

    exampleResource = rdfSingle(void.exampleResource)
    vocabulary = rdfMultiple(void.vocabulary)
    triples = rdfSingle(void.triples)

    distribution = rdfSingle(schema.distribution)
    licenseprop = rdfSingle(schema.license)

    alternateName = rdfMultiple(schema.alternateName)
    citation = rdfMultiple(schema.citation)

    keywords = rdfMultiple(schema.keywords)
    spatialCoverage = rdfSingle(schema.spatialCoverage)
    temporalCoverage = rdfSingle(schema.temporalCoverage)

    version = rdfSingle(schema.version)


class DataDownload(CreativeWork):
    rdf_type = schema.DataDownload

    contentUrl = rdfSingle(schema.contentUrl)
    encodingFormat = rdfSingle(schema.encodingFormat)


class ScholarlyArticle(CreativeWork):
    rdf_type = schema.ScholarlyArticle


class VisualArtwork(CreativeWork):
    rdf_type = schema.VisualArtwork

    artist = rdfMultiple(schema.artist)

    dateCreated = rdfSingle(schema.dateCreated)
    dateModified = rdfSingle(schema.dateModified)

    temporal = rdfSingle(schema.temporal)

    image = rdfSingle(schema.image)


class Place(Entity):
    rdf_type = schema.Place


class Person(Entity):
    rdf_type = schema.Person

    birthPlace = rdfSingle(schema.birthPlace)
    deathPlace = rdfSingle(schema.deathPlace)

    birthDate = rdfSingle(schema.birthDate)
    deathDate = rdfSingle(schema.deathDate)

    givenName = rdfSingle(schema.givenName)
    familyName = rdfSingle(schema.familyName)

    address = rdfMultiple(schema.address)
    hasOccupation = rdfMultiple(schema.hasOccupation)

    event = rdfMultiple(bio.event)

    birth = rdfSingle(bio.birth)
    death = rdfSingle(bio.death)

    hasName = rdfMultiple(pnv.hasName)


class PostalAddress(Entity):
    rdf_type = schema.PostalAddress

    streetAddress = rdfSingle(schema.streetAddress)


class Occupation(Entity):
    rdf_type = schema.Occupation


class Role(Entity):
    rdf_type = schema.Role

    address = rdfSingle(schema.address)
    hasOccupation = rdfSingle(schema.hasOccupation)

    startDate = rdfSingle(schema.startDate)
    endDate = rdfSingle(schema.endDate)

    hasTimeStamp = rdfSingle(sem.hasTimeStamp)
    hasBeginTimeStamp = rdfSingle(sem.hasBeginTimeStamp)
    hasEndTimeStamp = rdfSingle(sem.hasEndTimeStamp)
    hasEarliestBeginTimeStamp = rdfSingle(sem.hasEarliestBeginTimeStamp)
    hasLatestBeginTimeStamp = rdfSingle(sem.hasLatestBeginTimeStamp)
    hasEarliestEndTimeStamp = rdfSingle(sem.hasEarliestEndTimeStamp)
    hasLatestEndTimeStamp = rdfSingle(sem.hasLatestEndTimeStamp)


class PropertyValue(Entity):
    rdf_type = schema.PropertyValue

    value = rdfSingle(schema.value)


class PersonName(Entity):
    rdf_type = pnv.PersonName

    literalName = rdfSingle(pnv.literalName)
    givenName = rdfSingle(pnv.givenName)
    surnamePrefix = rdfSingle(pnv.surnamePrefix)
    baseSurname = rdfSingle(pnv.baseSurname)

    prefix = rdfSingle(pnv.prefix)
    disambiguatingDescription = rdfSingle(pnv.disambiguatingDescription)
    patronym = rdfSingle(pnv.patronym)
    surname = rdfSingle(pnv.surname)


#######
# BIO #
#######


class Event(rdfSubject):
    rdf_type = bio.Event, sem.Event
    label = rdfMultiple(RDFS.label)

    eventType = rdfSingle(sem.eventType)

    date = rdfSingle(bio.date)

    followingEvent = rdfSingle(bio.followingEvent)
    precedingEvent = rdfSingle(bio.precedingEvent)

    hasTimeStamp = rdfSingle(sem.hasTimeStamp)
    hasBeginTimeStamp = rdfSingle(sem.hasBeginTimeStamp)
    hasEndTimeStamp = rdfSingle(sem.hasEndTimeStamp)
    hasEarliestBeginTimeStamp = rdfSingle(sem.hasEarliestBeginTimeStamp)
    hasLatestBeginTimeStamp = rdfSingle(sem.hasLatestBeginTimeStamp)
    hasEarliestEndTimeStamp = rdfSingle(sem.hasEarliestEndTimeStamp)
    hasLatestEndTimeStamp = rdfSingle(sem.hasLatestEndTimeStamp)

    place = rdfSingle(bio.place)  # multi-predicates?

    witness = rdfMultiple(bio.witness)
    spectator = rdfMultiple(bio.spectator)
    parent = rdfMultiple(bio.parent)

    hasActor = rdfMultiple(sem.hasActor, range_type=sem.Role)

    comment = rdfSingle(RDFS.comment)


class EventType(rdfSubject):
    rdf_type = sem.EventType
    label = rdfMultiple(RDFS.label)


class IndividualEvent(Event):
    rdf_type = bio.IndividualEvent, sem.Event
    principal = rdfSingle(bio.principal)

    label = rdfMultiple(RDFS.label)


class GroupEvent(Event):
    rdf_type = bio.GroupEvent, sem.Event
    partner = rdfMultiple(bio.partner)

    label = rdfMultiple(RDFS.label)


class Birth(IndividualEvent):
    rdf_type = bio.Birth, sem.Event


class Baptism(IndividualEvent):
    rdf_type = bio.Baptism, sem.Event


class Burial(IndividualEvent):
    rdf_type = bio.Burial, sem.Event


class Death(IndividualEvent):
    rdf_type = bio.Death, sem.Event


class Resignation(IndividualEvent):
    rdf_type = bio.Resignation, sem.Event


class Marriage(GroupEvent):
    rdf_type = bio.Marriage, sem.Event


class Divorce(GroupEvent):
    rdf_type = bio.Divorce, sem.Event


class IntendedMarriage(GroupEvent):
    rdf_type = saa.IntendedMarriage
    hasDocument = rdfSingle(saa.hasDocument)


def main(loadData: dict, target: str = 'data/notarissennetwerk.trig'):
    """Main function that starts the download and conversion to RDF.

    Args:
        loadData (dict): notarissen data as dictionary
        target (str, optional): Destination file location. Defaults to
        'data/notarissennetwerk.trig'.
    """

    #######
    # RDF #
    #######

    toRDF(loadData, target=target)


def yearToDate(yearString):
    if yearString is None or yearString == "?" or '0000' in str(yearString):
        return None, None

    if type(yearString) == str and yearString.count('-') == 1:
        year, month = yearString.split('-')
        _, lastday = calendar.monthrange(int(year), int(month))

        beginDate = f"{year}-{month}-01"
        endDate = f"{year}-{month}-{str(lastday).zfill(1)}"

        return Literal(beginDate,
                       datatype=XSD.date), Literal(endDate, datatype=XSD.date)
    else:
        return Literal(f"{yearString}-01-01",
                       datatype=XSD.date), Literal(f"{yearString}-12-31",
                                                   datatype=XSD.date)


def street2adamlink(street, name2adamlink=name2adamlink):

    adamlink = None

    if street in name2adamlink:
        adamlink = URIRef(name2adamlink[street])
    else:
        matches = re.findall(r'\((.*)\)', street)
        if matches:
            adamlink = street2adamlink(matches[0])

    if adamlink is None and ',' in street:
        adamlink = street2adamlink(street.split(',')[0])
    elif adamlink is None and ' ' in street:
        adamlink = street2adamlink(street.rsplit(' ', 1)[0])

    return adamlink


def getSameAsPlace(placename: str) -> list:

    links = []

    # tgn
    tgn = place2tgn.get(placename)
    if tgn:
        links.append(URIRef(tgn))

    # ecartico
    ecartico = place2ecartico.get(placename)
    if ecartico:
        links.append(URIRef(ecartico))

    return links


def toRDF(d: dict, target: str):
    """Convert the earlier harvested and structured data to RDF.

    Args:
        d (dict): Dictionary from Notarissennetwerk
        target (str): Destination file path.
    """

    type2class = {
        None: None,
        '': None,
        'aanstelling': IndividualEvent,
        'admissie': IndividualEvent,
        'ambtsbeëindiging': Resignation,
        'begraven': Burial,
        'benoeming': IndividualEvent,
        'doop': Baptism,
        'faillissement': IndividualEvent,
        'geboren': Birth,
        'gescheiden': Divorce,
        'huwelijk': Marriage,
        'ondertrouw': IntendedMarriage,
        'overlijden': Death,
        'tijdelijk ambt gestaakt': IndividualEvent
    }

    type2label = {
        None: "",
        '': "",
        'aanstelling': 'aanstelling',
        'admissie': 'admissie',
        'ambtsbeëindiging': 'ambtsbeëindiging',
        'begraven': 'begrafenis',
        'benoeming': 'benoeming',
        'doop': 'doop',
        'faillissement': 'faillissement',
        'geboren': 'geboorte',
        'gescheiden': 'echtscheiding',
        'huwelijk': 'huwelijk',
        'ondertrouw': 'ondertrouw',
        'overlijden': 'overlijden',
        'tijdelijk ambt gestaakt': 'tijdelijke ambtsstaking'
    }

    rel2prop = {
        'achter-achterkleinzoon van': None,
        'achterkleinzoon van': None,
        'achterneef van': None,
        'betovergrootvader van': None,
        'broer van': rel.siblingOf,
        'grootvader van': rel.grandparentOf,
        'had als getuige': None,
        'had als klerk': None,
        'had als vertaler': None,
        'kind trouwde met kind van': None,
        'kleinzoon van': rel.grandchildOf,
        'neef van': None,
        'niet gespecificeerd': None,
        'oom van': None,
        'opgevolgd door': None,
        'opvolger van': None,
        'oudoom van': None,
        'overgrootvader van': None,
        'samenwerking met': rel.collaboratesWith,
        'schoonvader van': None,
        'schoonzoon van': None,
        'stiefvader van': None,
        'stiefzoon van': None,
        'vader van': rel.parentOf,
        'was getuige bij': None,
        'was klerk bij': None,
        'was vertaler bij': None,
        'zoon van': rel.childOf,
        'zwager van': None
    }

    rel2prop_inverse = {
        'achter-achterkleinzoon van': None,
        'achterkleinzoon van': None,
        'achterneef van': None,
        'betovergrootvader van': None,
        'broer van': rel.siblingOf,
        'grootvader van': rel.grandchildOf,
        'had als getuige': None,
        'had als klerk': None,
        'had als vertaler': None,
        'kind trouwde met kind van': None,
        'kleinzoon van': rel.grandparentOf,
        'neef van': None,
        'niet gespecificeerd': None,
        'oom van': None,
        'opgevolgd door': None,
        'opvolger van': None,
        'oudoom van': None,
        'overgrootvader van': None,
        'samenwerking met': rel.collaboratesWith,
        'schoonvader van': None,
        'schoonzoon van': None,
        'stiefvader van': None,
        'stiefzoon van': None,
        'vader van': rel.childOf,
        'was getuige bij': None,
        'was klerk bij': None,
        'was vertaler bij': None,
        'zoon van': rel.parentOf,
        'zwager van': None
    }

    ds = Dataset()
    dataset = ns.term('')

    g = rdfSubject.db = ds.graph(identifier=ns)

    type2eventType = {
        None:
        None,
        '':
        None,
        'aanstelling':
        EventType(nsEventType.term('aanstelling'),
                  label=[Literal('Aanstelling', lang='nl')]),
        'admissie':
        EventType(nsEventType.term('admissie'),
                  label=[Literal('Admissie', lang='nl')]),
        'ambtsbeëindiging':
        EventType(nsEventType.term('ambtsbeeindiging'),
                  label=[Literal('Ambtsbeëindiging', lang='nl')]),
        'begraven':
        EventType(nsEventType.term('begrafenis'),
                  label=[Literal('Begrafenis', lang='nl')]),
        'benoeming':
        EventType(nsEventType.term('benoeming'),
                  label=[Literal('Benoeming', lang='nl')]),
        'doop':
        EventType(nsEventType.term('doop'), label=[Literal('Doop',
                                                           lang='nl')]),
        'faillissement':
        EventType(nsEventType.term('faillissement'),
                  label=[Literal('Faillissement', lang='nl')]),
        'geboren':
        EventType(nsEventType.term('geboorte'),
                  label=[Literal('Geboorte', lang='nl')]),
        'gescheiden':
        EventType(nsEventType.term('scheiding'),
                  label=[Literal('Scheiding', lang='nl')]),
        'huwelijk':
        EventType(nsEventType.term('huwelijk'),
                  label=[Literal('Huwelijk', lang='nl')]),
        'ondertrouw':
        EventType(nsEventType.term('ondertrouw'),
                  label=[Literal('Ondertrouw', lang='nl')]),
        'overlijden':
        EventType(nsEventType.term('overlijden'),
                  label=[Literal('Overlijden', lang='nl')]),
        'tijdelijk ambt gestaakt':
        EventType(nsEventType.term('tijdelijkeambtsstaking'),
                  label=[Literal('Tijdelijke ambtsstaking', lang='nl')])
    }

    #############
    # Resources #
    #############

    for notary in d['notaries']:

        roleCounter = count(1)

        page = CreativeWork(URIRef(notary['uri']))

        if notary['place']:
            placeidentifier = "".join([
                i for i in notary['place']
                if i.lower() in 'abcdefghijklmnopqrstuvwxyz-'
            ])
            birthPlace = Place(nsPlace.term(placeidentifier),
                               name=[notary['place']],
                               sameAs=getSameAsPlace(notary['place']))
        else:
            birthPlace = None

        pn = PersonName(nsPersonName.term(str(notary['id'])),
                        prefix=notary['title'],
                        givenName=notary['firstName'],
                        patronym=notary['patronym'],
                        baseSurname=notary['lastName'],
                        surnamePrefix=notary['prefix'],
                        literalName=notary['name'],
                        label=[notary['name']])

        if notary['prefix']:
            familyName = notary['prefix'].capitalize(
            ) + ' ' + notary['lastName']
        else:
            familyName = notary['lastName']

        p = Person(nsPerson.term(str(notary['id'])),
                   name=[notary['name']],
                   hasName=[pn],
                   givenName=notary['firstName'],
                   familyName=familyName,
                   birthPlace=birthPlace)

        # identifiers
        identifiers = []
        ## protocol
        if notary['section_id'] and notary['col_id'] == 5075:
            identifier = PropertyValue(
                None,
                name=[Literal("Protocol Notarieel Archief", lang="nl")],
                value=str(notary['section_id']))
            identifiers.append(identifier)

            notaryData = notarissenEAD[str(notary['section_id'])]

            uri = notaryData['uri']
            p.url = uri

            inventoryCodes = zip(notaryData['inventories'],
                                 notaryData['codes'])

            for inv, code in inventoryCodes:
                b = InventoryBook(URIRef(inv), name=[code], author=[p])

        ## repertorium
        if notary['rep_id']:
            identifier = PropertyValue(
                None,
                name=[Literal("Repertorium", lang="nl")],
                value=str(notary['rep_id']))
            identifiers.append(identifier)

        p.identifier = identifiers

        page.mainEntity = p
        p.mainEntityOfPage = page

        names = []
        for n in notary['name_variants']:
            names.append(n['name'])
        p.alternateName = names

        # Adresses
        addresses = []
        for n, a in enumerate(notary['addresses'], 1):
            startDate = Literal(a['from'], datatype=XSD.gYear,
                                normalize=False) if a['from'] else None
            endDate = Literal(a['to'], datatype=XSD.gYear,
                              normalize=False) if a['to'] else None

            earliestBeginTimeStamp, latestBeginTimeStamp = yearToDate(
                a['from'])
            earliestEndTimeStamp, latestEndTimeStamp = yearToDate(a['to'])

            adamlink = street2adamlink(a['street'])

            if adamlink is None:
                print(a['street'])

            address = PostalAddress(nsAddress.term(f"{notary['id']}-{n}"),
                                    streetAddress=a['street'],
                                    name=[a['street']],
                                    closeMatch=adamlink)  # TODO: Adamlink

            r = Role(nsRole.term(f"{notary['id']}-{next(roleCounter)}"),
                     startDate=startDate,
                     endDate=endDate,
                     address=address,
                     name=[a['street']],
                     hasEarliestBeginTimeStamp=earliestBeginTimeStamp,
                     hasLatestBeginTimeStamp=latestBeginTimeStamp,
                     hasEarliestEndTimeStamp=earliestEndTimeStamp,
                     hasLatestEndTimeStamp=latestEndTimeStamp)

            addresses.append(r)

        # Events
        lifeEvents = []
        for nEvent, e in enumerate(notary['events'], 1):

            EventClass = type2class[e['type']]
            eventTypeLabel = type2label.get(e['type'], "").title()
            eventType = type2eventType[e['type']]

            if EventClass:
                if e['date'] and e['date'] not in  ('0000', '0000-00-00'):

                    yearLabel = e['date'][:4]

                    try:
                        date = datetime.datetime.fromisoformat(
                            e['date']).date()
                        date = Literal(date, datatype=XSD.date)

                        timeStamp = date
                        beginTimeStamp = date
                        endTimeStamp = date
                        earliestBeginTimeStamp = date
                        latestBeginTimeStamp = date
                        earliestEndTimeStamp = date
                        latestEndTimeStamp = date

                    except:
                        if e['date'].endswith('00-00') or len(e['date']) == 4:
                            date = Literal(e['date'][:4],
                                           datatype=XSD.gYear,
                                           normalize=False)

                            timeStamp = None
                            beginTimeStamp = None
                            endTimeStamp = None
                            earliestBeginTimeStamp, latestEndTimeStamp = yearToDate(
                                e['date'][:4])
                            earliestEndTimeStamp, latestEndTimeStamp = yearToDate(
                                e['date'][:4])

                        elif e['date'].endswith('-00') or len(e['date']) == 7:
                            date = Literal(e['date'][:7],
                                           datatype=XSD.gYearMonth,
                                           normalize=False)

                            timeStamp = None
                            beginTimeStamp = None
                            endTimeStamp = None
                            earliestBeginTimeStamp, latestEndTimeStamp = yearToDate(
                                e['date'][:7])
                            earliestEndTimeStamp, latestEndTimeStamp = yearToDate(
                                e['date'][:7])

                        else:
                            print(e['date'])
                else:
                    date = None
                    yearLabel = "?"

                if e['place']:
                    placeidentifier = "".join([
                        i for i in e['place']
                        if i.lower() in 'abcdefghijklmnopqrstuvwxyz-'
                    ])
                    place = Place(nsPlace.term(placeidentifier),
                                  name=[e['place']],
                                  sameAs=getSameAsPlace(e['place']))
                else:
                    place = None

                o = EventClass(
                    nsEvent.term(f"{notary['id']}-{nEvent}"),
                    eventType=eventType,
                    date=date,
                    hasTimeStamp=timeStamp,
                    hasBeginTimeStamp=beginTimeStamp,
                    hasEndTimeStamp=endTimeStamp,
                    hasEarliestBeginTimeStamp=earliestBeginTimeStamp,
                    hasLatestBeginTimeStamp=latestEndTimeStamp,
                    hasEarliestEndTimeStamp=earliestEndTimeStamp,
                    hasLatestEndTimeStamp=latestEndTimeStamp,
                    place=place,
                    label=[f"{eventTypeLabel} van {p.name[0]} ({yearLabel})"])
                try:
                    o.principal = p
                except AttributeError:
                    o.partner = [p]

                lifeEvents.append(o)

                if EventClass == Birth:
                    p.birth = o
                    p.birthDate = date
                elif EventClass == Death:
                    p.death = o
                    p.deathDate = date

        p.address = addresses
        p.event = lifeEvents

        # Occupations
        occupations = []
        for occ in notary['jobs']:
            startDate = Literal(occ['from'],
                                datatype=XSD.gYear,
                                normalize=False) if occ['from'] else None
            endDate = Literal(occ['to'], datatype=XSD.gYear,
                              normalize=False) if occ['to'] else None

            earliestBeginTimeStamp, latestBeginTimeStamp = yearToDate(
                occ['from'])
            earliestEndTimeStamp, latestEndTimeStamp = yearToDate(occ['to'])

            occupation = Occupation(nsOccupation.term("".join([
                i for i in occ['details']
                if i.lower() in 'abcdefghijklmnopqrstuvwxyz-'
            ])),
                                    name=[occ['details']])

            r = Role(nsRole.term(f"{notary['id']}-{next(roleCounter)}"),
                     startDate=startDate,
                     endDate=endDate,
                     hasOccupation=occupation,
                     name=[occ['details']],
                     hasEarliestBeginTimeStamp=earliestBeginTimeStamp,
                     hasLatestBeginTimeStamp=latestBeginTimeStamp,
                     hasEarliestEndTimeStamp=earliestEndTimeStamp,
                     hasLatestEndTimeStamp=latestEndTimeStamp)

            occupations.append(r)

        p.hasOccupation = occupations

        # Portrait

        if notary['portrait']:
            if notary['portrait'].startswith(
                    'https://notarissennetwerk.nl/images/'):
                portraituri = URIRef(urllib.parse.quote(notary['portrait']))
                portrait = VisualArtwork(None, about=p, image=portraituri)
            else:
                portrait = VisualArtwork(URIRef(notary['portrait']), about=p)
            p.subjectOf = [portrait]

        # Relations
        for relation in notary['relations']:
            prop = rel2prop[relation['type']]
            propInverse = rel2prop_inverse[relation['type']]

            if prop is None:
                prop = schema.knows
            if propInverse is None:
                propInverse = schema.knows

            # prop = schema.knows
            obj = nsPerson.term(str(relation['id']))

            g.add((p.resUri, prop, obj))
            g.add((obj, propInverse, p.resUri))

    for prop in list(rel2prop.values()) + list(rel2prop_inverse.values()):
        if prop:
            g.add((prop, RDFS.subPropertyOf, schema.knows))

    ########
    # Meta #
    ########

    rdfSubject.db = ds

    ds.bind('owl', OWL)
    ds.bind('dcterms', dcterms)
    ds.bind('create', create)
    ds.bind('schema', schema)
    ds.bind('sem', sem)
    ds.bind('void', void)
    ds.bind('foaf', foaf)
    ds.bind('bio', bio)
    ds.bind('skos', SKOS)
    ds.bind('pnv', pnv)
    ds.bind('rel', rel)

    ds.serialize(target, format='trig')


if __name__ == "__main__":

    # DATA = 'data/notarissen.json'
    # with open(DATA) as infile:
    #     DATA = json.load(infile)

    DATA = requests.get(
        "https://notarissennetwerk.nl/notarissen/export/json").json()

    TARGET = 'trig/notarissennetwerk.trig'

    main(loadData=DATA, target=TARGET)
