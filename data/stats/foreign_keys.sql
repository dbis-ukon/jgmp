

ALTER TABLE badges
ADD CONSTRAINT fk_badges_users
FOREIGN KEY (userid)
REFERENCES users(id);

ALTER TABLE comments
ADD CONSTRAINT fk_comments_posts
FOREIGN KEY (postid)
REFERENCES posts(id);

ALTER TABLE comments
ADD CONSTRAINT fk_comments_users
FOREIGN KEY (userid)
REFERENCES users(id);

ALTER TABLE posthistory
ADD CONSTRAINT fk_posthistory_posts
FOREIGN KEY (postid)
REFERENCES posts(id);

ALTER TABLE posthistory
ADD CONSTRAINT fk_posthistory_users
FOREIGN KEY (userid)
REFERENCES users(id);

ALTER TABLE postlinks
ADD CONSTRAINT fk_postlinks_posts_postid
FOREIGN KEY (postid)
REFERENCES posts(id);

ALTER TABLE postlinks
ADD CONSTRAINT fk_postlinks_posts_relatedpostid
FOREIGN KEY (relatedpostid)
REFERENCES posts(id);

ALTER TABLE posts
ADD CONSTRAINT fk_posts_users_owneruserid
FOREIGN KEY (owneruserid)
REFERENCES users(id);

ALTER TABLE posts
ADD CONSTRAINT fk_posts_users_lasteditoruserid
FOREIGN KEY (lasteditoruserid)
REFERENCES users(id);

ALTER TABLE tags
ADD CONSTRAINT fk_tags_posts
FOREIGN KEY (excerptpostid)
REFERENCES posts(id);

ALTER TABLE votes
ADD CONSTRAINT fk_votes_posts
FOREIGN KEY (postid)
REFERENCES posts(id);

ALTER TABLE votes
ADD CONSTRAINT fk_votes_users
FOREIGN KEY (userid)
REFERENCES users(id);