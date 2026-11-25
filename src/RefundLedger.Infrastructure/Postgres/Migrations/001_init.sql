create table accounts (
    id            bigint generated always as identity primary key,
    code          text    not null unique,
    currency      char(3) not null,
    mode          text    not null check (mode in ('strict', 'delta')),
    -- null: порога нет
    floor_minor   bigint,
    balance_minor bigint  not null default 0,
    unique (id, currency),
    -- Последний рубеж: даже ошибка в коде не уведёт счёт ниже порога.
    constraint balance_above_floor check (floor_minor is null or balance_minor >= floor_minor)
) with (fillfactor = 70);

create table postings (
    id         bigint generated always as identity primary key,
    key        text        not null unique,
    currency   char(3)     not null,
    booked_at  timestamptz not null default now(),
    unique (id, currency)
);

-- Составные внешние ключи не дают смешать валюты внутри проводки.
create table entries (
    id           bigint generated always as identity,
    posting_id   bigint      not null,
    account_id   bigint      not null,
    currency     char(3)     not null,
    amount_minor bigint      not null check (amount_minor <> 0),
    booked_at    timestamptz not null default now(),
    primary key (id, booked_at),
    foreign key (posting_id, currency) references postings (id, currency),
    foreign key (account_id, currency) references accounts (id, currency)
) partition by range (booked_at);

create table entries_default partition of entries default;
create index entries_account_idx on entries (account_id, id desc);
create index entries_posting_idx on entries (posting_id);

create function forbid_change() returns trigger language plpgsql as $$
begin
    raise exception 'Журнал неизменяем: исправление делается новой проводкой';
end $$;

create function ensure_entries_partition(month_start date) returns void language plpgsql as $$
declare
    name text := 'entries_' || to_char(month_start, 'YYYY_MM');
begin
    if to_regclass(name) is null then
        execute format('create table %I partition of entries for values from (%L) to (%L)',
            name, month_start::timestamptz, (month_start + interval '1 month')::timestamptz);
        -- Триггер truncate, в отличие от строчных, с родителя на партицию не переходит.
        execute format('create trigger no_truncate before truncate on %I for each statement execute function forbid_change()', name);
    end if;
end $$;

-- Границы партиций считаются в UTC независимо от настроек сервера.
set local timezone = 'UTC';
select ensure_entries_partition(date_trunc('month', now())::date);
select ensure_entries_partition((date_trunc('month', now()) + interval '1 month')::date);

-- Баланс проводки проверяется в конце транзакции, когда записаны все строки.
create function check_posting_balanced() returns trigger language plpgsql as $$
declare
    pid bigint;
begin
    if tg_argv[0] = 'posting' then pid := new.id; else pid := new.posting_id; end if;
    if (select coalesce(sum(amount_minor), 0) <> 0 or count(*) < 2
        from entries where posting_id = pid) then
        raise exception 'Проводка % не сбалансирована', pid;
    end if;
    return null;
end $$;

create constraint trigger postings_balanced after insert on postings
    deferrable initially deferred for each row execute function check_posting_balanced('posting');
-- Второй триггер нужен от строки, дописанной в уже существующую проводку.
create constraint trigger entries_balanced after insert on entries
    deferrable initially deferred for each row execute function check_posting_balanced('entry');

create trigger entries_immutable before update or delete on entries
    for each row execute function forbid_change();
create trigger postings_immutable before update or delete on postings
    for each row execute function forbid_change();
create trigger no_truncate before truncate on entries for each statement execute function forbid_change();
create trigger no_truncate before truncate on entries_default for each statement execute function forbid_change();
create trigger no_truncate before truncate on postings for each statement execute function forbid_change();

-- Очередь дельт вместо «водяного знака» по id: id выдаётся раньше коммита,
-- и знак навсегда пропустил бы транзакцию, закоммиченную с опозданием.
create table balance_deltas (
    id           bigint generated always as identity primary key,
    account_id   bigint not null references accounts (id),
    amount_minor bigint not null
);

create table refunds (
    id           uuid primary key,
    buyer_id     bigint      not null,
    seller_id    bigint      not null,
    amount_minor bigint      not null check (amount_minor > 0),
    seller_minor bigint      not null check (seller_minor between 0 and amount_minor),
    currency     char(3)     not null,
    status       smallint    not null,
    version      int         not null,
    created_at   timestamptz not null default now()
);

create table payouts (
    id              uuid primary key,
    refund_id       uuid        not null references refunds (id),
    amount_minor    bigint      not null check (amount_minor > 0),
    currency        char(3)     not null,
    status          smallint    not null,
    attempts        int         not null,
    next_attempt_at timestamptz not null,
    lease_token     uuid,
    lease_until     timestamptz,
    psp_reference   text,
    reject_reason   text,
    trace_parent    text,
    sent_at         timestamptz,
    registry_seen_at timestamptz,
    created_at      timestamptz not null default now()
);

-- На возврат — не больше одной живой или успешной выплаты (5 — отклонена).
create unique index payouts_one_active on payouts (refund_id) where status <> 5;
-- Частичные индексы: завершённых выплат много, а очереди короткие.
create index payouts_to_send on payouts (next_attempt_at) where status = 1;
create index payouts_to_resolve on payouts (next_attempt_at) where status in (2, 3);

create table outbox (
    id           bigint generated always as identity primary key,
    message_key  text        not null,
    type         text        not null,
    payload      jsonb       not null,
    trace_parent text,
    created_at   timestamptz not null default now()
);

create table registry_cursor (
    id       int primary key check (id = 1),
    last_seq bigint not null
);
insert into registry_cursor values (1, 0);

create table reconciliation_issues (
    psp_seq     bigint primary key,
    payout_key  uuid        not null,
    kind        text        not null,
    details     text        not null,
    detected_at timestamptz not null default now()
);
