create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
AS
set nocount on
begin
	--добавлены ;
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal);
	-- Рекомендуется при объявлении типов не использовать длину поля  max 
	declare @ErrorMessage varchar(max);

-- Проверка на корректность загрузки
	if not exists (
		--не хватало отсупов
		select 1
		from syn.ImportFile as f
		where f.ID = @ID_Record
			and f.FlagLoaded = cast(1 as bit)
	)
	-- лишний отступ
	begin
		set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных';
		exec syn.usp_LogError @ErrorMessage, 3, 1;
		RETURN;
	end

	-- Чтение из слоя временных данных
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs
		-- указан вид join 
		INNER join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		INNER join dbo.Season as s on s.Name = cs.Season
		INNER join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and c_dist.ID_mapping_DataSource = 1
		-- При соединение двух таблиц, сперва после  on  указываем поле присоединяемой таблицы
		INNER join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not NULL;

	-- Определяем некорректные записи
		
		-- проверить отступы
		
	-- Добавляем причину, по которой запись считается некорректной
	select
		cs.*
		,case
		-- лишние отступы перед when
		 when c.ID is null 
		 	-- не было отступа после then
		 	then 'UID клиента отсутствует в справочнике "Клиент"'
		 when c_dist.ID is null 
		 	then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
		 when s.ID is null 
		 	then 'Сезон отсутствует в справочнике "Сезон"'
		 when cst.ID is null 
		 	then 'Тип клиента отсутствует в справочнике "Тип клиента"'
		 when try_cast(cs.DateBegin as date) is null 
		 	then 'Невозможно определить Дату начала'
		 when try_cast(cs.DateEnd as date) is null 
			then 'Невозможно определить Дату окончания'
		 when try_cast(isnull(cs.FlagActive, 0) as bit) is null 
		 	then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
		-- перед join добавить отступы
		left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor 
			-- and на новую строку с отступом от join
			and c_dist.ID_mapping_DataSource = 1
		left join dbo.Season as s on s.Name = cs.Season
		left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where c.ID is null
		or c_dist.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	-- into не указывается
	merge syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	-- then перенесен на одну строку с when
	when matched and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update
		set 
			-- пропущен отступ после set
			ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive);

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)
		raiserror(@ErrorMessage, 1, 1)

		--Формирование таблицы для отчетности
		select top 100
			bir.Season as 'Сезон'
			,bir.UID_DS_Customer as 'UID Клиента'
			,bir.Customer as 'Клиент'
			,bir.CustomerSystemType as 'Тип клиента'
			,bir.UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,bir.CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(bir.DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateBegin) as 'Дата начала'
			,isnull(format(try_cast(birDateEnd as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateEnd) as 'Дата окончания'
			,bir.FlagActive as 'Активность'
			,bir.Reason as 'Причина'
		from #BadInsertedRows as bir

		return
	end
end
