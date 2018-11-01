import React from "react";
import { Box, Flex } from "grid-styled";

import QuestionAndResultLoader from "metabase/containers/QuestionAndResultLoader";
import Visualization from "metabase/visualizations/components/Visualization";

import Icon, { IconWrapper } from "metabase/components/Icon";

class ResultPanel extends React.Component {
  render() {
    return (
      <Box className="border-top bg-white overflow-hidden full-height">
        <Flex
          align="center"
          py={2}
          justify="center"
          w={"100%"}
          className="text-brand-hover cursor-pointer"
        >
          <Icon name="table" mt={1} />
        </Flex>
        <Box
          className="relative"
          style={{ height: "40%", width: "100%" }}
          mx={3}
        >
          <QuestionAndResultLoader questionId={this.props.dataId}>
            {({ question, result, cancel, reload, rawSeries, loading }) => {
              return (
                <Box className="spread flex flex-column">
                  {rawSeries && <Visualization rawSeries={rawSeries} />}
                </Box>
              );
            }}
          </QuestionAndResultLoader>
        </Box>
        <Box
          className="relative"
          style={{ height: "40%", width: "100%" }}
          mx={3}
          mb={3}
        >
          <QuestionAndResultLoader questionId={this.props.aggregationId}>
            {({ question, result, cancel, reload, rawSeries, loading }) => {
              if (!question) {
                return <div>"Loading..."</div>;
              }
              return (
                <Box className="spread flex flex-column">
                  {rawSeries && <Visualization rawSeries={rawSeries} />}
                </Box>
              );
            }}
          </QuestionAndResultLoader>
        </Box>
      </Box>
    );
  }
}

export default ResultPanel;
