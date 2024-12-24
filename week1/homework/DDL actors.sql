create table if not exists public.actors
(
    actor         text    not null,
    actorid       text,
    films         films[],
    quality_class quality_class,
    is_active     boolean,
    current_year  integer not null,
    primary key (actor, current_year)
);

alter table public.actors
    owner to postgres;

