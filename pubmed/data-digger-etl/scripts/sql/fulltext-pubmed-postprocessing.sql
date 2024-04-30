-- Add Location column to linktable
alter table linktable add location text;

-- Update location column in linktable with the s3 location
update linktable lt set location = ftd.location from fulltextdownloads ftd where ftd.identifier = lt.pmc;