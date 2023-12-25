ALTER TABLE aka_name 
ADD CONSTRAINT fk_aka_name_name
FOREIGN KEY (person_id)
REFERENCES name(id);

--Alter aka_title in order to allow adding a foreign key constraint
ALTER TABLE aka_title ALTER COLUMN movie_id DROP NOT NULL;

UPDATE aka_title
SET movie_id = NULL
WHERE movie_id = 0;

ALTER TABLE aka_title 
ADD CONSTRAINT fk_aka_title_title
FOREIGN KEY (movie_id)
REFERENCES title(id);

ALTER TABLE cast_info 
ADD CONSTRAINT fk_cast_info_name
FOREIGN KEY (person_id)
REFERENCES name(id);

ALTER TABLE cast_info 
ADD CONSTRAINT fk_cast_info_title
FOREIGN KEY (movie_id)
REFERENCES title(id);

ALTER TABLE cast_info 
ADD CONSTRAINT fk_cast_info_char_name
FOREIGN KEY (person_role_id)
REFERENCES char_name(id);

ALTER TABLE cast_info
ADD CONSTRAINT fk_cast_info_role_type
FOREIGN KEY (role_id)
REFERENCES role_type(id);

ALTER TABLE complete_cast 
ADD CONSTRAINT fk_complete_cast_title
FOREIGN KEY (movie_id)
REFERENCES title(id);

ALTER TABLE complete_cast 
ADD CONSTRAINT fk_complete_cast_comp_cast_type
FOREIGN KEY (subject_id)
REFERENCES comp_cast_type(id);

ALTER TABLE complete_cast 
ADD CONSTRAINT fk_complete_cast_comp_cast_type2
FOREIGN KEY (status_id)
REFERENCES comp_cast_type(id);

ALTER TABLE movie_companies 
ADD CONSTRAINT fk_movie_companies_title
FOREIGN KEY (movie_id)
REFERENCES title(id);

ALTER TABLE movie_companies 
ADD CONSTRAINT fk_movie_companies_company_name
FOREIGN KEY (company_id)
REFERENCES company_name(id);

ALTER TABLE movie_companies 
ADD CONSTRAINT fk_movie_companies_company_type
FOREIGN KEY (company_type_id)
REFERENCES company_type(id);

ALTER TABLE movie_info 
ADD CONSTRAINT fk_movie_info_title
FOREIGN KEY (movie_id)
REFERENCES title(id);

ALTER TABLE movie_info 
ADD CONSTRAINT fk_movie_info_info_type
FOREIGN KEY (info_type_id)
REFERENCES info_type(id);

ALTER TABLE movie_info_idx
ADD CONSTRAINT fk_movie_info_idx_title
FOREIGN KEY (movie_id)
REFERENCES title(id);

ALTER TABLE movie_info_idx
ADD CONSTRAINT fk_movie_info_idx_info_type
FOREIGN KEY (info_type_id)
REFERENCES info_type(id);

ALTER TABLE movie_keyword
ADD CONSTRAINT fk_movie_keyword_title
FOREIGN KEY (movie_id)
REFERENCES title(id);

ALTER TABLE movie_keyword
ADD CONSTRAINT fk_movie_keyword_keyword
FOREIGN KEY (keyword_id)
REFERENCES keyword(id);

ALTER TABLE movie_link
ADD CONSTRAINT fk_movie_link_title
FOREIGN KEY (movie_id)
REFERENCES title(id);

ALTER TABLE movie_link
ADD CONSTRAINT fk_movie_link_title2
FOREIGN KEY (linked_movie_id)
REFERENCES title(id);

ALTER TABLE movie_link
ADD CONSTRAINT fk_movie_link_link_type
FOREIGN KEY (link_type_id)
REFERENCES link_type(id);

ALTER TABLE person_info
ADD CONSTRAINT fk_person_info_name
FOREIGN KEY (person_id)
REFERENCES name(id);

ALTER TABLE person_info
ADD CONSTRAINT fk_person_info_info_type
FOREIGN KEY (info_type_id)
REFERENCES info_type(id);

ALTER TABLE title
ADD CONSTRAINT fk_title_kind_type
FOREIGN KEY (kind_id)
REFERENCES kind_type(id);

ALTER TABLE title
ADD CONSTRAINT fk_title_title
FOREIGN KEY (episode_of_id)
REFERENCES title(id);