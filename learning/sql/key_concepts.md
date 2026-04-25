## Non-sargable predicate
Regula generala:
- Nu se aplica functii pe coloana din conditia de join
- Orice functie: UPPER, CAST, ISNULL, YEAR()... pe coloana din **ON** sau 
**WHERE** invalideaza indexul!
### Exemple de cod prost
```sql
-- ❌ ISNULL pe coloana de join
SELECT c.ContractNumber, p.Amount
FROM Contracts c
JOIN Payments p on ISNULL(c.ContractNumber, '') = ISNULL(p.ContractNumber, '')

-- ❌ YEAR() pe coloană indexată
SELECT * FROM Payments
WHERE YEAR(PaymentDate) = 2024
```
### Cand se pot aplica functii si e ok?
- Cand nu sunt pe o coloana indexata
- Pe coloane din **SELECT** / **ORDER BY**

## Inner vs Left Join
### Capcane subtile
```sql
-- ❌ Ai vrut LEFT JOIN, dar filtrul din WHERE îl transformă în INNER JOIN
SELECT c.ContractNumber, p.Amount
FROM Contracts c
         LEFT JOIN Payments p ON c.ContractId = p.ContractId
WHERE p.Amount > 1000
-- WHERE elimina randurile cu NULL, ce se dorea in LEFT JOIN
-- adica exact contractele fara plati — pe care voiai sa le pastrezi

-- ✅ Condiția e în ON, nu în WHERE
SELECT c.ContractNumber, p.Amount
FROM Contracts c
         LEFT JOIN Payments p ON c.ContractId = p.ContractId
    AND p.Amount > 1000
-- Apar contractele fara plati, cu NULL pe Amount
```

## Indexare: Cand ajuta, cand strica
Index = o structura separata pe disc, ordonata dupa coloana indexata,
care ajuta SQL Server sa gaseasca randuri fara sa scaneze toata tabela

Fara index = full table scan
Cu index = index seek, sare direct la randurile relevante

### Tipuri de indecsi

**Clustered** = defineste ordinea fizica a datelor din tabela, o tabela poate
avea doar unul, de obicei fiind pe **primary key**
```sql
CREATE TABLE Contracts(
    ContractId INT PRIMARY KEY, -- acesta este clustered index
    ContractNumber VARCHAR(50),
    ClientId INT
)
```

**Non-Clustered** = structura separata, defineste pointeri catre randul
din tabela
```sql
CREATE INDEX IX_Contracts_ClientId ON Contracts(ClientId)
```

### Cand indexul ajuta ✅
```sql
-- Cautare dupa coloana indexata ✅
SELECT * FROM Contracts WHERE ClientId = 1234

-- Join pe coloana indexata ✅
-- daca ClientId e indexat in ambele tabele ⚠️
SELECT * FROM Contracts c
JOIN Clients cl ON c.ClientId = cl.ClientId

-- Order by pe coloana indexata, evita sortarea in memorie ✅
SELECT * FROM Contracts c
ORDER BY c.ClientId
```

### Cand indexul nu ajuta ❌
```sql
-- Functie pe coloana indexata ❌
WHERE YEAR(PaymentDate) = 2024

-- Like cu wildcard la inceput ❌
WHERE ContractNumber LIKE '%123%' -- full scan ❌
WHERE ContractNumber LIKE '123%' -- asta poate folosi indexul ✅

-- Negatie - IS NOT NULL, NOT IN, != forteaza scan de obicei ❌
WHERE Status != 'ACTIVE'

-- Selectivitate slaba: index pe coloana cu 2, 3 valori distincte ❌
-- Status e ACTIV/INACTIV
CREATE INDEX IX_Contracts_Status ON Contracts(Status) -- inutil
```










