#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import os
import uuid
from datetime import datetime

# Libs
from tinydb import TinyDB, where
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinyrecord import transaction
from tinydb_serialization import SerializationMiddleware
from tinydb_smartcache import SmartCacheTable

# Custom
from .abstract import AbstractRecords
from .datetime_serialization import DateTimeSerializer, TimeDeltaSerializer

##################
# Configurations #
##################


#####################################
# Base Data Storage Class - Records #
#####################################

class Records(AbstractRecords):
    """ 
    Automates CRUD operations on a structured TinyDB database. Operations are
    atomicised using TinyRecord transactions, queries are smart cahced

    Attributes:
        db_path (str): Path to json source
    
    Args:
        db_path (str): Path to json source
        *subjects: All subject types pertaining to records
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    ###########
    # Helpers #
    ###########

    def load_database(self):
        """ Loads json source as a TinyDB database, configured to cache queries,
            I/O operations, as well as serialise datetimes objects if necessary.
            Subjects are initialised as tables of the database

        Returns:
            database (TinyDB)
        """
        serialization = SerializationMiddleware(JSONStorage)
        serialization.register_serializer(DateTimeSerializer(), 'TinyDate')
        serialization.register_serializer(TimeDeltaSerializer(), 'TinyDelta')

        database = TinyDB(
            path=self.db_path, 
            sort_keys=True,
            indent=4,
            #separators=(',', ': '),
            storage=CachingMiddleware(serialization)
        )

        database.table_class = SmartCacheTable

        return database

    ##################
    # Core Functions #
    ##################

    def create(self, subject, key, new_record):
        """ Creates a new record in a specified subject table within database

        Args:  
            subject (str): Table to be operated on
            new_record (dict): Information for creating a new record
            key (str): Primary key of the current table
        Returns:
            New record added (tinydb.database.Document)
        """
        database = self.load_database()

        with database as db:

            subject_table = db.table(subject)

            with transaction(subject_table) as tr:

                # Remove additional digits (eg. microseconds)
                date_created = datetime.strptime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "%Y-%m-%d %H:%M:%S"
                )
                new_record['created_at'] = date_created

                if subject_table.contains(where(key) == new_record[key]):
                    tr.update(new_record, where(key) == new_record[key])

                else:
                    tr.insert(new_record)

            record = subject_table.get(where(key) == new_record[key])

        return record


    def read_all(self, subject):
        """ Retrieves all records in a specified table of the database

        Args:
            subject (str): Table to be operated on
        Returns:
            Records (list(tinydb.database.Document))
        """
        database = self.load_database()

        with database as db:
            subject_table = db.table(subject)
            records = [record for record in iter(subject_table)]

        return records


    def read(self, subject, key, r_id):
        """ Retrieves a single record from a specified table in the database

        Args:  
            subject (str): Table to be operated on
            key (str): Primary key of the current table
            r_id (dict): Identifier of specified records
        Returns:
            Specified record (tinydb.database.Document)
        """
        database = self.load_database()

        with database as db:
            subject_table = db.table(subject)
            record = subject_table.get(where(key) == r_id)

        return record


    def update(self, subject, key, r_id, updates):
        """ Updates an existing record with specified updates

        Args:  
            subject (str): Table to be operated on
            key (str): Primary key of the current table
            r_id (dict): Identifier of specified records
            updates (dict): New key-value pairs to update existing record with
        Returns:
            Updated record (tinydb.database.Document)
        """
        database = self.load_database()

        with database as db:

            subject_table = db.table(subject)

            with transaction(subject_table) as tr:

                tr.update(updates, where(key) == r_id)

            updated_record = subject_table.get(where(key) == r_id)

        return updated_record
        

    def delete(self, subject, key, r_id):
        """ Deletes a specified record from the specified table in the database

        Args:
            subject (str): Table to be operated on
            key (str): Primary key of the current table
            r_id (dict): Identifier of specified records
        Returns:
            Deleted record (tinydb.database.Document)
        """
        database = self.load_database()

        with database as db:

            subject_table = db.table(subject)

            record = subject_table.get(where(key) == r_id)

            with transaction(subject_table) as tr:

                tr.remove(where(key) == r_id)
            
            assert not subject_table.get(where(key) == r_id)
        
        return record




############################################
# Base Data Storage Class - TopicalRecords #
############################################

class TopicalRecords(Records):
    """ Customised class to handle relations within the PySyft REST-PRC service
    Args:
        subject (str): Main subject type of records
        identifier (str): Identifying key of record
        db_path (str): Path to json source
        *relations (list(str)): All subjects related to specified subject
    """

    def __init__(self, subject, identifier, db_path, *relations):
        self.subject = subject
        self.identifier = identifier
        self.relations = relations
        super().__init__(db_path=db_path)

    ###########
    # Helpers #
    ###########
    
    def _get_related_metadata(self, r_id, key):
        """ Retrieves all related records from specified relations
        
        Args:
            r_id (dict(str, str)): 
                Record Identifier implemented as a composite collection of keys
            key (str): Key to be used as a unique composite identifier
        Returns:
            Collection of all related records (dict(str,list(Document)))
        """
        database = self.load_database()

        with database as db:

            all_related_records = {}
            for subject in self.relations:
                related_table = db.table(subject)
                related_records = related_table.search(
                    where(key)[self.identifier] == r_id[self.identifier]
                )
                all_related_records[subject] = related_records

        return all_related_records

    def _expand_record(self, record, key):
        """ Adds additional metadata from related subjects to specified record

        Args:
            record (tinydb.database.Document): Record to be expanded
            key (str): Key to be used as a unique composite identifier
        Returns:
            Expanded record (tinydb.database.Document)
        """
        r_id = record[key]
        related_records = self._get_related_metadata(r_id, key)
        record['relations'] = related_records
        return record

    ##################
    # Core Functions #
    ##################

    def create(self, new_record):
        return super().create(self.subject, "key", new_record)


    def read_all(self, f_key="key", filter={}):
        """ Retrieves entire collection of records, with an option to filter out
            ones with specific key-value pairs.

        Args:
            f_key (str): Foreign key for extracting relations
            filter (dict(str,str)): Key-value pairs for filtering records
        Returns:
            Filtered records (list(tinydb.database.Document))
        """
        all_records = super().read_all(self.subject)
        expanded_records = []
        for record in all_records:
            if (
                (not filter.items() <= record['key'].items()) and
                (not filter.items() <= record[f_key].items()) and
                (not filter.items() <= record.items())
            ):
                continue
            exp_record = self._expand_record(record, f_key)
            expanded_records.append(exp_record)
        return expanded_records


    def read(self, r_id, f_key="key"):
        main_record = super().read(self.subject, "key", r_id)
        if main_record:
            return self._expand_record(main_record, f_key)
        return main_record


    def update(self, r_id, updates):
        assert not ("key" in updates.keys())
        return super().update(self.subject, "key", r_id, updates)


    def delete(self, r_id, key="key"):
        """ Uses composite keys for efficient cascading deletion of child 
            relations in related subjects

        Args:
            r_id (dict(str, str)): 
                Record Identifier implemented as a composite collection of keys
        Returns:
            Deleted record + related records deleted (dict)
        """
        database = self.load_database()

        with database as db:

            # Archive record targeted for deletion for output
            subject_table = db.table(self.subject)
            main_record = subject_table.get(where(key) == r_id)
            expanded_record = self._expand_record(main_record, key)

            for subject in self.relations:
                related_table = db.table(subject)

                # Perform cascading deletion of all involved relations
                with transaction(related_table) as related_tr:
                    related_tr.remove(
                        where(key)[self.identifier] == r_id[self.identifier]
                    )

                # Check that only the correct records are deleted 
                related_records = expanded_record['relations'][subject]
                for r_record in related_records:
                    assert related_table.get(doc_id=r_record.doc_id) is None
                    assert r_record[key][self.identifier] == r_id[self.identifier]

            # Finally, delete main entry itself
            with transaction(subject_table) as main_tr:
                main_tr.remove(where(key) == r_id)

            assert subject_table.get(doc_id=main_record.doc_id) is None
            assert main_record[key] == r_id
        
        return main_record



################################################
# Base Data Storage Class - AssociationRecords #
################################################

class AssociationRecords(TopicalRecords):
    """ The difference between associations and relations is that associations
        can be thought of as a "reversed" relation. It is used to map 
        dependencies upstream. 
        
        An example of structural relation is Project -> Experiment -> Run. A
        project can access details in their experiments and runs. An experiment
        can only access all runs, but not their parent projects. A run can only
        access its own attributes.

        An example of structural association is Registration <= Tag <= Alignment
        An alignment can accumulate links from its associated tags and
        registrations. A tag can accumulate links only from its associated 
        registrations. A registration does not accumulate links from other 
        entities. However, hierachy is still maintained in that these 
        accumulated links are used to manage attribute access as in a structural
        relation: Registration -> Tag -> Alignment, using accumulated links

        IMPORTANT: AssociationRecords CANNOT modify downstream relations defined
                   in its parent class TopicalRecords
    """
    def __init__(
        self, 
        subject, 
        identifier, 
        db_path, 
        relations=[], 
        *associations
    ):
        self.associations =  associations
        super().__init__(
            subject,  
            identifier, 
            db_path,
            *relations
        )

    ###########
    # Helpers #
    ###########

    def __generate_link(self):
        return {self.identifier: uuid.uuid1().hex}

    ##################
    # Core Functions #
    ##################

    def create(self, new_record):
        link = self.__generate_link()
        # Use key to trace down associated records and store their links
        key = new_record['key']
        database = self.load_database()
        with database as db:
            for subject in self.associations:
                associated_table = db.table(subject)
                associated_records = associated_table.search(
                    where('key') == key
                )
                assert len(associated_records) <= 1
                for associated_record in associated_records:
                    ext_link = associated_record['link']
                    link.update(ext_link)
        # Store all links alongside record details
        new_record['link'] = link
        return super().create(new_record)


    def read_all(self, filter={}):
        return super().read_all(f_key="link", filter=filter)


    def read(self, r_id):
        return super().read(r_id, f_key="link")


    def update(self, r_id, updates):
        assert not ("link" in updates.keys())
        return super().update(r_id, updates)


    def delete(self, r_id):
        """ Switches to composite link keys for performing deletion cascade """
        # Search for record using r_id
        database = self.load_database()

        with database as db:
            subject_table = db.table(self.subject)
            main_record = subject_table.get(where('key') == r_id)

        link = main_record['link']
        return super().delete(link, key="link")