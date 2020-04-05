function resp = symbol_atr(apikey, symbol, varargin)

if nargin >= 3
	if any(strcmp(["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"], varargin{1}))
		interval = varargin{1};
	else
		error('Invalid interval.');
	end
else
	interval = 'daily';
end

if nargin >= 4
	time_period = varargin{2};
else
	time_period = 14;
end

if nargin >= 5
	datelimit = datetime(varargin{3});
else
	datelimit = datetime(2010, 1, 1);
end

raw_resp = webread(strcat('https://www.alphavantage.co/query?function=ATR',...
	'&symbol=', symbol,...
	'&interval=', num2str(interval),...
	'&time_period=', num2str(time_period),...
	'&apikey=', apikey));

time_values = fieldnames(raw_resp.TechnicalAnalysis_ATR);
datelimit = days252bus(datelimit,datetime(strrep(time_values{1}(2:end),'_','-')))+1;

resp.symbol = raw_resp.MetaData.x1_Symbol;
resp.data = cell(min(length(time_values), datelimit), 2);

for i = 1:size(resp.data,1)
	resp.data(i,:) = {strrep(time_values{i}(2:end),'_','-'),...
										str2double(raw_resp.TechnicalAnalysis_ATR.(time_values{i}).ATR)};
end

end