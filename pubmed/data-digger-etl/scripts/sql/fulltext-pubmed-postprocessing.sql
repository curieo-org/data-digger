-- Add Location column to linktable
alter table linktable add column if not exists location text;

-- Update location column in linktable with the s3 location
update linktable lt set location = ftd.location from fulltextdownloads ftd where ftd.identifier = lt.pmc;