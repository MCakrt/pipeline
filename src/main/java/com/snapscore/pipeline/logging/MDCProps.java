package com.snapscore.pipeline.logging;

import org.slf4j.MDC;

import java.util.*;

public class MDCProps {

    private final static String CLASS_NAME_KEY = "class_name";
    private final static String COMPONENT_KEY = "component";
    private final static String SPORT_KEY = "sport";
    private final static String PROVIDER_KEY = "provider";
    private final static String EVENT_TYPE_KEY = "event_type";
    private final static String EVENT_ID_OF_PROVIDER_KEY_PATTERN = "event_id_prov_%s";
    private final static String STAGE_ID_OF_PROVIDER_KEY_PATTERN = "stage_id_prov_%s";
    private final static String TEAM_ID_OF_PROVIDER_KEY_PATTERN = "team_id_prov_%s_"; // tailing underscore is on purpose
    private final static String PLAYER_ID_KEY = "player_id";
    private final static String ANY_ID_KEY_PATTERN = "any_id_%s";
    private final static String EXECUTION_KEY = "execution";
    private final static String ANALYTICS_ID_KEY = "analytics_id";

    private String className;
    private String component;
    private String sport;
    private String provider;
    private String eventType;
    private Map<Integer, String> stageIdsByProvider;
    private Map<Integer, String> eventIdsByProvider;
    private List<Map<Integer, String>> teamIdsByProviderList;
    private String playerId;
    private List<String> anyIdList;
    private String execution;
    private String analyticsId;

    // Package private on purpose because he fields need to be mutable sp we want very controlled access to instantiation and usage.
    MDCProps() {
    }

    private MDCProps(String className,
                     String component,
                     String sport,
                     String provider,
                     String eventType,
                     Map<Integer, String> stageIdsByProvider,
                     Map<Integer, String> eventIdsByProvider,
                     List<Map<Integer, String>> teamIdsByProviderList,
                     String playerId,
                     List<String> anyIdList,
                     String execution,
                     String analyticsId) {
        this.className = className;
        this.component = component;
        this.sport = sport;
        this.provider = provider;
        this.eventType = eventType;
        this.stageIdsByProvider = stageIdsByProvider;
        this.eventIdsByProvider = eventIdsByProvider;
        this.teamIdsByProviderList = teamIdsByProviderList;
        this.playerId = playerId;
        this.anyIdList = anyIdList;
        this.execution = execution;
        this.analyticsId = analyticsId;
    }

    MDCProps merge(MDCProps other) {
        mergeLists(this.teamIdsByProviderList, other.teamIdsByProviderList);
        return new MDCProps(
                other.className != null ? other.className : this.className,
                other.component != null ? other.component : this.component,
                other.sport != null ? other.sport : this.sport,
                other.provider != null ? other.provider : this.provider,
                other.eventType != null ? other.eventType : this.eventType,
                mergeMaps(this.stageIdsByProvider, other.stageIdsByProvider),
                mergeMaps(this.eventIdsByProvider, other.eventIdsByProvider),
                mergeLists(this.teamIdsByProviderList, other.teamIdsByProviderList),
                other.playerId != null ? other.playerId : this.playerId,
                other.anyIdList != null && !other.anyIdList.isEmpty() ? other.anyIdList : this.anyIdList,
                other.execution != null ? other.execution : this.execution,
                other.analyticsId != null ? other.analyticsId : this.analyticsId
        );
    }

    Map<Integer, String> mergeMaps(Map<Integer, String> map1, Map<Integer, String> map2) {
        if (map1 == null && map2 == null) {
            return null;
        } else {
            Map<Integer, String> resultMap = new HashMap<>(2);
            if (map1 != null) {
                resultMap.putAll(map1);
            }
            if (map2 != null) {
                resultMap.putAll(map2);
            }
            return resultMap;
        }
    }

    List<Map<Integer, String>> mergeLists(List<Map<Integer, String>> list1, List<Map<Integer, String>> list2) {
        if (list1 == null && list2 == null) {
            return null;
        } else {
            List<Map<Integer, String>> resultList = new ArrayList<>(2);
            if (list1 != list2) { // to avoid duplications of very same lists
                if (list1 != null) {
                    resultList.addAll(list1);
                }
                if (list2 != null) {
                    resultList.addAll(list2);
                }
            } else {
                if (list1 != null) {
                    resultList.addAll(list1);
                }
            }
            return resultList;
        }
    }

    MDCProps copy() {
        return new MDCProps().merge(this);
    }


    void setPropsToMDC() {
        if (this.className != null) MDC.put(CLASS_NAME_KEY, this.className);
        if (this.component != null) MDC.put(COMPONENT_KEY, this.component);
        if (this.sport != null) MDC.put(SPORT_KEY, this.sport);
        if (this.provider != null) MDC.put(PROVIDER_KEY, this.provider);
        if (this.eventType != null) MDC.put(EVENT_TYPE_KEY, this.eventType);
        setMDCEntityIds(this.eventIdsByProvider, EVENT_ID_OF_PROVIDER_KEY_PATTERN);
        setMDCEntityIds(this.stageIdsByProvider, STAGE_ID_OF_PROVIDER_KEY_PATTERN);
        setMDCTeamIds();
        if (this.playerId != null) MDC.put(PLAYER_ID_KEY, this.playerId);
        setMDCAnyListValues(anyIdList, MDCProps.ANY_ID_KEY_PATTERN);
        if (this.execution != null) MDC.put(EXECUTION_KEY, this.execution);
        if (this.analyticsId != null) MDC.put(ANALYTICS_ID_KEY, this.analyticsId);
    }

    private void setMDCTeamIds() {
        if (this.teamIdsByProviderList != null) {
            List<Map<Integer, String>> teamIdsByProviderList = this.teamIdsByProviderList;
            for (int teamIdx = 0; teamIdx < teamIdsByProviderList.size(); teamIdx++) {
                Map<Integer, String> teamIdsByProvider = teamIdsByProviderList.get(teamIdx);
                String keyPattern = MDCProps.TEAM_ID_OF_PROVIDER_KEY_PATTERN + teamIdx;
                setMDCEntityIds(teamIdsByProvider, keyPattern);
            }
        }
    }

    private void setMDCEntityIds(Map<Integer, String> idsByProvider, String keyPattern) {
        if (idsByProvider != null && !idsByProvider.isEmpty()) {
            idsByProvider.forEach((providerId, eventId) -> {
                String kay = String.format(keyPattern, providerId);
                setMDCProviderEventId(kay, eventId);
            });
        }
    }

    private void setMDCProviderEventId(String providerId, String eventId) {
        if (providerId != null && eventId != null) {
            MDC.put(providerId, eventId);
        }
    }

    private void setMDCAnyListValues(List<String> valueList, String keyPattern) {
        if (valueList != null) {
            for (int valueIdx = 0; valueIdx < valueList.size(); valueIdx++) {
                String value = valueList.get(valueIdx);
                String key = String.format(keyPattern, valueIdx);
                MDC.put(key, value);
            }
        }
    }


    public MDCProps cls(Class<?> clazz) {
        this.className = clazz.getSimpleName();
        return this;
    }

    /**
     * Can be a string which helps you identify the code path or method or class or component inside the code base
     */
    public MDCProps comp(String component) {
        this.component = component;
        return this;
    }

    public MDCProps sp(String sport) {
        this.sport = sport;
        return this;
    }

    public MDCProps prov(String provider) {
        this.provider = provider;
        return this;
    }

    public MDCProps evTy(String eventType) {
        this.eventType = eventType;
        return this;
    }

    /**
     * Used for the execution time, counts etc ... so the passed values should be stringified numbers
     */
    public MDCProps exec(String execution) {
        this.execution = execution;
        return this;
    }

    /**
     * Used for the a unique descriptive expression that can be reliably used for analytical purposes
     * in kibana (like charts etc...)
     *
     * Example analyticsId value: "ignoring_duplicate_request"
     */
    public MDCProps analyticsId(String analyticsId) {
        this.analyticsId = analyticsId;
        return this;
    }


    // STAGE IDS ========================================================

    // useful for ids from ProviderValue
    public MDCProps stgIds(Map<Integer, String> stageIdsByProvider) {
        this.stageIdsByProvider = stageIdsByProvider != null ? new HashMap<>(stageIdsByProvider) : Collections.emptyMap();
        return this;
    }

    public MDCProps stgId(int stageId, int providerId) {
        putStageId(toString(stageId), providerId);
        return this;
    }

    public MDCProps stgId(String stageId, int providerId) {
        putStageId(stageId, providerId);
        return this;
    }

    public MDCProps stgIdInternal(int stageId) {
        putStageId(toString(stageId), 1);
        return this;
    }

    public MDCProps stgIdInternal(String stageId) {
        putStageId(stageId, 1);
        return this;
    }

    public MDCProps stgIdEnet(int stageId) {
        putStageId(toString(stageId), 2);
        return this;
    }

    public MDCProps stgIdEnet(String stageId) {
        putStageId(stageId, 2);
        return this;
    }

    private void putStageId(String stageId, int providerId) {
        if (stageId != null && providerId > 0) {
            if (this.stageIdsByProvider == null) {
                this.stageIdsByProvider = new HashMap<>(2);
            }
            this.stageIdsByProvider.put(providerId, stageId);
        }
    }

    // EVENT IDS ========================================================

    // useful for ids from ProviderValue
    public MDCProps eIds(Map<Integer, String> eventIdsByProvider) {
        this.eventIdsByProvider = eventIdsByProvider != null ? new HashMap<>(eventIdsByProvider) : Collections.emptyMap();
        return this;
    }

    public MDCProps eId(String eventId, int providerId) {
        putEventId(eventId, providerId);
        return this;
    }

    public MDCProps eId(int eventId, int providerId) {
        putEventId(toString(eventId), providerId);
        return this;
    }

    // provider specific convenience methods

    public MDCProps eIdEnet(String eventId) {
        putEventId(eventId, 2);
        return this;
    }

    public MDCProps eIdEnet(int eventId) {
        putEventId(toString(eventId), 2);
        return this;
    }

    public MDCProps eIdInternal(String eventId) {
        putEventId(eventId, 1);
        return this;
    }

    public MDCProps eIdInternal(int eventId) {
        putEventId(toString(eventId), 1);
        return this;
    }

    private void putEventId(String eventId, int providerId) {
        if (eventId != null && providerId > 0) {
            if (this.eventIdsByProvider == null) {
                this.eventIdsByProvider = new HashMap<>(2);
            }
            this.eventIdsByProvider.put(providerId, eventId);
        }
    }

    // TEAM IDS ========================================================

    public MDCProps tIds(Map<Integer, String>... teamIdsByProviders) {
        if (teamIdsByProviders != null) {
            List<Map<Integer, String>> allTeamIdslist = new ArrayList<>(teamIdsByProviders.length);
            for (Map<Integer, String> eventIdsByProvider : teamIdsByProviders) {
                Map<Integer, String> copy = eventIdsByProvider != null ? new HashMap<>(eventIdsByProvider) : Collections.emptyMap();
                allTeamIdslist.add(copy);
            }
            this.teamIdsByProviderList = allTeamIdslist;
        }
        return this;
    }

    public MDCProps tId(String teamId, int providerId) {
        putTeamId(teamId, providerId);
        return this;
    }

    public MDCProps tId(int teamId, int providerId) {
        putTeamId(toString(teamId), providerId);
        return this;
    }

    public MDCProps tIdInternal(String teamId) {
        putTeamId(teamId, 1);
        return this;
    }

    public MDCProps tIdInternal(int teamId) {
        putTeamId(toString(teamId), 1);
        return this;
    }

    public MDCProps tIdEnet(String teamId) {
        putTeamId(teamId, 2);
        return this;
    }

    public MDCProps tIdEnet(int teamId) {
        putTeamId(toString(teamId), 2);
        return this;
    }

    public MDCProps tIds(List<String> teamIds, int providerId) {
        if (teamIds != null) {
            for (String teamId : teamIds) {
                putTeamId(teamId, providerId);
            }
        }
        return this;
    }

//    public MDCProps tIds(List<Integer> teamIds, int providerId) {
//        if (teamIds != null) {
//            for (Integer teamId : teamIds) {
//                putTeamId(toString(teamId), providerId);
//            }
//        }
//        return this;
//    }


    private void putTeamId(String teamId, int providerId) {
        if (teamId != null && providerId > 0) {
            if (this.teamIdsByProviderList == null) {
                this.teamIdsByProviderList = new ArrayList<>(2);
            }
            Map<Integer, String> teamIdByProvider = new HashMap<>(2);
            teamIdByProvider.put(providerId, teamId);
            this.teamIdsByProviderList.add(teamIdByProvider);
        }
    }


    // PLAYER IDS ========================================================

    public MDCProps pId(String playerId) {
        this.playerId = playerId;
        return this;
    }

    public MDCProps pId(int playerId) {
        this.playerId = toString(playerId);
        return this;
    }


    // ANY IDS ========================================================

    // Use specialised methods for event, team, and stage ids!
    // Use this ONLY if there is no specialised method and it does not make sense to add it
    public MDCProps anyId(long anyId) {
        putAnyId(toString(anyId));
        return this;
    }

    // Use specialised methods for event, team, and stage ids!
    // Use this ONLY if there is no specialised method and it does not make sense to add it
    public MDCProps anyId(String anyId) {
        putAnyId(anyId);
        return this;
    }

    private void putAnyId(String anyId) {
        if (anyId != null) {
            if (this.anyIdList == null) {
                this.anyIdList = new ArrayList<>(2);
            }
            this.anyIdList.add(anyId);
        }
    }

    // OTHER ... ========================================================

    private String toString(int intValue) {
        return String.valueOf(intValue);
    }

    private String toString(long longValue) {
        return String.valueOf(longValue);
    }


}
