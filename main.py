"""
Pipeline that converts the data from https://notarissennetwerk.nl into RDF.

Built upon an adapted version of RDFAlchemy for Python (3.7+). Install with:

```bash
pip install git+https://github.com/LvanWissen/RDFAlchemy.git
```

Questions:
    Leon van Wissen (l.vanwissen@uva.nl)

"""

import os
import time
import datetime
import json
import re
from itertools import count

from unidecode import unidecode

import requests
from bs4 import BeautifulSoup

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

rdflib.graph.DATASET_DEFAULT_GRAPH_ID = create

ns = Namespace("https://data.create.humanities.uva.nl/id/notarissennetwerk/")

with open('data/name2adamlink.json') as infile:
    name2adamlink = json.load(infile)


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
    identifier = rdfSingle(schema.identifier)


class CreativeWork(Entity):
    rdf_type = schema.CreativeWork

    publication = rdfMultiple(schema.publication)
    author = rdfMultiple(schema.author)

    text = rdfSingle(schema.text)

    mainEntity = rdfSingle(schema.mainEntity)


class DatasetClass(Entity):

    # db = ConjunctiveGraph

    rdf_type = void.Dataset, schema.Dataset

    title = rdfMultiple(dcterms.title)
    description = rdfMultiple(dcterms.description)
    descriptionSchema = rdfMultiple(schema.description)
    creator = rdfMultiple(schema.creator)
    publisher = rdfMultiple(dcterms.publisher)
    publisherSchema = rdfMultiple(schema.publisher)
    contributor = rdfMultiple(dcterms.contributor)
    contributorSchema = rdfMultiple(schema.contributor)
    source = rdfSingle(dcterms.source)
    isBasedOn = rdfSingle(schema.isBasedOn)
    date = rdfSingle(dcterms.date)
    dateCreated = rdfSingle(schema.dateCreated)
    created = rdfSingle(dcterms.created)
    issued = rdfSingle(dcterms.issued)
    modified = rdfSingle(dcterms.modified)

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


bio = Namespace("http://purl.org/vocab/bio/0.1/")
sem = Namespace("http://semanticweb.cs.vu.nl/2009/11/sem/")
saa = Namespace("https://data.goldenagents.org/datasets/SAA/ontology/")

#######
# BIO #
#######


class Event(rdfSubject):
    rdf_type = bio.Event
    label = rdfMultiple(RDFS.label)

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


class IndividualEvent(Event):
    rdf_type = bio.IndividualEvent
    principal = rdfSingle(bio.principal)

    label = rdfMultiple(RDFS.label)


class GroupEvent(Event):
    rdf_type = bio.GroupEvent
    partner = rdfMultiple(bio.partner)

    label = rdfMultiple(RDFS.label)


class Birth(IndividualEvent):
    rdf_type = bio.Birth


class Baptism(IndividualEvent):
    rdf_type = bio.Baptism


class Burial(IndividualEvent):
    rdf_type = bio.Burial


class Death(IndividualEvent):
    rdf_type = bio.Death


class Resignation(IndividualEvent):
    rdf_type = bio.Resignation


class Marriage(GroupEvent):
    rdf_type = bio.Marriage


class Divorce(GroupEvent):
    rdf_type = bio.Divorce


class IntendedMarriage(GroupEvent):
    rdf_type = saa.IntendedMarriage
    hasDocument = rdfSingle(saa.hasDocument)


def main(loadData: str = None, target: str = 'data/notarissennetwerk.trig'):
    """Main function that starts the scraping and conversion to RDF.

    Args:
        loadData (str, optional): File pointer to a json file with earlier
        scraped data. If supplied, the data will not be fetched again.
        Defaults to None.
        target (str, optional): Destination file location. Defaults to
        'data/notarissennetwerk.trig'.
    """

    with open(loadData, 'r', encoding='utf-8') as infile:
        DATA = json.load(infile)

    #######
    # RDF #
    #######

    toRDF(DATA, target=target)


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


def toRDF(d: dict, target: str):
    """Convert the earlier harvested and structured data to RDF.

    Args:
        d (dict): Dictionary with structured portrait information, coming from
        the loadData() function.
        target (str): Destination file path.
    """

    type2class = {
        None: None,
        '': None,
        'aanstelling': None,
        'admissie': None,
        'ambtsbeëindiging': Resignation,
        'begraven': Burial,
        'benoeming': None,
        'doop': Baptism,
        'faillissement': None,
        'geboren': Birth,
        'gescheiden': Divorce,
        'huwelijk': Marriage,
        'ondertrouw': IntendedMarriage,
        'overlijden': Death,
        'tijdelijk ambt gestaakt': None
    }

    rel2prop = {
        'achter-achterkleinzoon van': None,
        'achterkleinzoon van': None,
        'achterneef van': None,
        'betovergrootvader van': None,
        'broer van': None,
        'grootvader van': None,
        'had als getuige': None,
        'had als klerk': None,
        'had als vertaler': None,
        'kind trouwde met kind van': None,
        'kleinzoon van': None,
        'neef van': None,
        'niet gespecificeerd': None,
        'oom van': None,
        'opgevolgd door': None,
        'opvolger van': None,
        'oudoom van': None,
        'overgrootvader van': None,
        'samenwerking met': None,
        'schoonvader van': None,
        'schoonzoon van': None,
        'stiefvader van': None,
        'stiefzoon van': None,
        'vader van': None,
        'was getuige bij': None,
        'was klerk bij': None,
        'was vertaler bij': None,
        'zoon van': None,
        'zwager van': None
    }

    rel2prop_inverse = {
        'achter-achterkleinzoon van': None,
        'achterkleinzoon van': None,
        'achterneef van': None,
        'betovergrootvader van': None,
        'broer van': None,
        'grootvader van': None,
        'had als getuige': None,
        'had als klerk': None,
        'had als vertaler': None,
        'kind trouwde met kind van': None,
        'kleinzoon van': None,
        'neef van': None,
        'niet gespecificeerd': None,
        'oom van': None,
        'opgevolgd door': None,
        'opvolger van': None,
        'oudoom van': None,
        'overgrootvader van': None,
        'samenwerking met': None,
        'schoonvader van': None,
        'schoonzoon van': None,
        'stiefvader van': None,
        'stiefzoon van': None,
        'vader van': None,
        'was getuige bij': None,
        'was klerk bij': None,
        'was vertaler bij': None,
        'zoon van': None,
        'zwager van': None
    }

    ds = Dataset()
    dataset = ns.term('')

    g = rdfSubject.db = ds.graph(identifier=ns)

    #############
    # Resources #
    #############

    for notary in d['notaries']:

        if notary['place']:
            placeidentifier = "".join([
                i for i in notary['place']
                if i.lower() in 'abcdefghijklmnopqrstuvwxyz-'
            ])
            birthPlace = Place(BNode(placeidentifier), name=[notary['place']])
        else:
            birthPlace = None

        p = Person(URIRef(notary['uri']),
                   name=[notary['name']],
                   givenName=notary['firstName'],
                   familyName=notary['lastName'],
                   birthPlace=birthPlace,
                   identifier=int(notary['rep_id'])
                   if notary['rep_id'] != 0 else None)

        names = []
        for n in notary['name_variants']:
            names.append(n['name'])
        p.alternateName = names

        # Adresses
        addresses = []
        for a in notary['addresses']:
            startDate = Literal(a['from'],
                                datatype=XSD.gYear) if a['from'] else None
            endDate = Literal(a['to'], datatype=XSD.gYear) if a['to'] else None

            adamlink = street2adamlink(a['street'])

            if adamlink is None:
                print(a['street'])

            address = PostalAddress(None,
                                    streetAddress=a['street'],
                                    closeMatch=adamlink)  # TODO: Adamlink

            r = Role(None,
                     startDate=startDate,
                     endDate=endDate,
                     address=address,
                     name=[a['street']])

            addresses.append(r)

        # Events
        lifeEvents = []
        for nEvent, e in enumerate(notary['events'], 1):

            EventClass = type2class[e['type']]
            if EventClass:
                if e['date']:
                    try:
                        date = datetime.datetime.fromisoformat(
                            e['date']).date()
                        date = Literal(date, datatype=XSD.date)
                    except:
                        if e['date'].endswith('00-00') or len(e['date']) == 4:
                            date = Literal(e['date'][:4], datatype=XSD.gYear)
                        elif e['date'].endswith('-00') or len(e['date']) == 7:
                            date = Literal(e['date'][:7],
                                           datatype=XSD.gYearMonth)
                        else:
                            print(e['date'])
                else:
                    date = None

                if e['place']:
                    placeidentifier = "".join([
                        i for i in e['place']
                        if i.lower() in 'abcdefghijklmnopqrstuvwxyz-'
                    ])
                    place = Place(BNode(placeidentifier), name=[e['place']])
                else:
                    place = None

                o = EventClass(
                    None,  #URIRef(notary['uri'] + f"#event-{nEvent}")
                    date=date,
                    place=place)
                try:
                    o.principal = p
                except AttributeError:
                    o.partner = [p]

                lifeEvents.append(o)

                if EventClass == Birth:
                    p.birth = o
                elif EventClass == Death:
                    p.death = o

        p.address = addresses
        p.event = lifeEvents

        # Occupations
        occupations = []
        for occ in notary['jobs']:
            startDate = Literal(occ['from'],
                                datatype=XSD.gYear) if occ['from'] else None
            endDate = Literal(occ['to'],
                              datatype=XSD.gYear) if occ['to'] else None

            occupation = Occupation(None, name=[occ['details']])

            r = Role(None,
                     startDate=startDate,
                     endDate=endDate,
                     hasOccupation=occupation,
                     name=[occ['details']])

            occupations.append(r)

        p.hasOccupation = occupations

        # Relations
        for rel in notary['relations']:
            # prop = rel2prop[rel['type']]
            prop = foaf.knows
            obj = URIRef(rel['uri'])

            g.add((p.resUri, prop, obj))
            g.add((obj, prop, p.resUri))

    ########
    # Meta #
    ########

    rdfSubject.db = ds

    description = """"""

    contributors = ""
    download = DataDownload(
        None,
        contentUrl=URIRef("http://example.com"),
        # name=Literal(),
        url=URIRef("http://example.com"),
        encodingFormat="application/trig")

    date = Literal(datetime.datetime.now().strftime('%Y-%m-%d'),
                   datatype=XSD.datetime)

    contributors = contributors.split(', ')

    creators = []

    dataset = DatasetClass(
        ns.term(''),
        name=[Literal("Notarissennetwerk", lang='nl')],
        about=None,
        url=URIRef('https://notarissennetwerk.nl/'),
        description=[Literal(description, lang='nl')],
        descriptionSchema=[Literal(description, lang='nl')],
        creator=creators,
        publisher=[URIRef("https://leonvanwissen.nl/me")],
        publisherSchema=[URIRef("https://leonvanwissen.nl/me")],
        contributor=contributors,
        contributorSchema=contributors,
        source=URIRef('https://notarissennetwerk.nl/'),
        isBasedOn=URIRef('https://notarissennetwerk.nl/'),
        date=date,
        dateCreated=date,
        distribution=download,
        created=None,
        issued=None,
        modified=None,
        exampleResource=p,
        vocabulary=[
            URIRef("http://schema.org/"),
            URIRef("http://semanticweb.cs.vu.nl/2009/11/sem/"),
            URIRef("http://xmlns.com/foaf/0.1/")
        ],
        triples=sum(1 for i in ds.graph(identifier=ns).subjects()),
        version="1.0",
        licenseprop=URIRef("https://creativecommons.org/licenses/by-sa/4.0/"))

    ds.bind('owl', OWL)
    ds.bind('dcterms', dcterms)
    ds.bind('create', create)
    ds.bind('schema', schema)
    ds.bind('sem', sem)
    ds.bind('void', void)
    ds.bind('foaf', foaf)
    ds.bind('bio', bio)
    ds.bind('skos', SKOS)

    ds.serialize(target, format='trig')


if __name__ == "__main__":

    DATA = 'data/notarissen.json'
    TARGET = 'data/notarissennetwerk.trig'

    if os.path.exists(DATA):
        main(loadData=DATA, target=TARGET)
    else:
        main(loadData=None, target=TARGET)
